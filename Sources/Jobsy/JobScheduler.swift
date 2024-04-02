//
//  JobScheduler.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

import Foundation
@preconcurrency import RediStack

public actor JobScheduler {
	let redis: RedisConnection
	let queue: String
	let kindMap: [String: any Job.Type]

	let encoder = JSONEncoder()
	let decoder = JSONDecoder()

	enum Error: Swift.Error {
		case invalidJobKind, noIntervalFound
	}

	public init(redis: RedisConnection, kinds: [any Job.Type], queue: String = "default") {
		self.redis = redis
		self.queue = queue
		self.kindMap = kinds.reduce(into: [:]) { result, kind in
			result[String(describing: kind)] = kind
		}
	}

	var keys: RedisKeys {
		RedisKeys(queue: queue)
	}

	public var isConnected: Bool {
		redis.isConnected
	}

	public func errored() async throws -> [ErroredJob] {
		let errored = try await redis.lrange(from: keys.errored, firstIndex: 0, lastIndex: -1, as: Data.self).get()

		return try errored.compactMap { data in
			if let data {
				return try decoder.decode(ErroredJob.self, from: data)
			} else {
				return nil
			}
		}
	}

	public func perform(_ job: any Job) async throws {
		do {
			try await job.perform()
		} catch {
			let erroredJob = ErroredJob(
				id: UUID().uuidString,
				jobID: job.id,
				error: error.localizedDescription
			)

			_ = try await redis.lpush(encoder.encode(erroredJob), into: keys.errored).get()
		}
	}

	// Queues a job. If a frequency is specified, it's added to the scheduled queue, otherwise it just goes on the main queue.
	// TODO: Maybe look into redis streams?
	public func push(
		_ job: some Job,
		performAt: Date? = nil,
		frequency: JobFrequency? = nil
	) async throws {
		let serializedJob = try SerializedJob(
			id: job.id,
			kind: job.kind,
			parameters: encoder.encode(job.parameters)
		)

		let serializedJobData = try encoder.encode(serializedJob)

		try await transaction {
			_ = redis.set(keys.job(job.id), to: serializedJobData)

			if let performAt {
				_ = redis.zadd([(job.id, performAt.timeIntervalSince1970)], to: keys.scheduled)
			} else {
				_ = redis.lpush(job.id, into: keys.jobs)
			}

			switch frequency {
			case .times(let count, let duration):
				_ = redis.set(keys.remaining(job.id), to: count)
				_ = redis.set(keys.interval(job.id), to: duration.components.seconds)
			case .forever(let duration):
				_ = redis.set(keys.remaining(job.id), to: -1)
				_ = redis.set(keys.interval(job.id), to: duration.components.seconds)
			default:
				()
			}
		}
	}

	// Moves scheduled jobs to the main queue. If a job has repeats, it schedules a new one after that job's interval.
	public func schedule(now: Date? = nil) async throws {
		let now = now ?? Date()

		let jobIDsToSchedule = try await redis.zrangebyscore(from: keys.scheduled, withScores: 0...now.timeIntervalSince1970).get().compactMap(\.string)

		if jobIDsToSchedule.isEmpty {
			return
		}

		try await transaction {
			_ = redis.zrem(jobIDsToSchedule, from: keys.scheduled)
			_ = redis.lpush(jobIDsToSchedule, into: keys.jobs)
		}

		for jobID in jobIDsToSchedule {
			guard let remaining = try await redis.get(keys.remaining(jobID), as: Int.self).get() else {
				continue
			}

			guard let interval = try await redis.get(keys.interval(jobID), as: Int.self).get() else {
				throw Error.noIntervalFound
			}

			if remaining == -1 {
				// This job repeats forever
				_ = try await redis.zadd([(jobID, now.addingTimeInterval(TimeInterval(interval)).timeIntervalSince1970)], to: keys.scheduled).get()
			} else if remaining == 0 {
				_ = try await redis.delete(keys.all(for: jobID)).get()
			} else {
				try await transaction {
					_ = redis.zadd([(jobID, now.addingTimeInterval(TimeInterval(interval)).timeIntervalSince1970)], to: keys.scheduled)
				}
			}
		}
	}

	public func pop() async throws -> (any Job)? {
		// TODO: move to a different working queue?
		guard let nextJobID = try await redis.rpop(from: keys.jobs, as: String.self).get() else {
			return nil
		}

		guard let serializedJobData = try await redis.get(keys.job(nextJobID), as: Data.self).get() else {
			return nil
		}

		let remaining = try? await redis.decrement(keys.remaining(nextJobID)).get()
		if remaining == nil ||  remaining == 0 {
			_ = try await redis.delete(keys.all(for: nextJobID)).get()
		}

		let serializedJob = try decoder.decode(SerializedJob.self, from: serializedJobData)
		guard let kind = kindMap[serializedJob.kind] else {
			throw Error.invalidJobKind
		}

		return try serializedJob.decode(kind, with: decoder)
	}

	@discardableResult private func transaction(block: () -> Void) async throws -> RESPValue {
		_ = redis.send(command: "MULTI", with: [])

		block()

		return try await redis.send(command: "EXEC").get()
	}
}
