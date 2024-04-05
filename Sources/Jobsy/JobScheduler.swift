//
//  JobScheduler.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

import Foundation
import Logging
@preconcurrency import RediStack

public actor JobScheduler {
	let redis: RedisConnection
	let queue: String
	let kindMap: [String: any Job.Type]

	let encoder = JSONEncoder()
	let decoder = JSONDecoder()
	
	nonisolated(unsafe) public var logger: Logger? {
		didSet {
			if let logger {
				logger.notice("JobScheduler logger set")
			}
		}
	}

	nonisolated public static let onceRemaining = -2
	nonisolated public static let foreverRemaining = -1

	enum Error: Swift.Error {
		case invalidJobKind, noIntervalFound
	}

	public init(redis: RedisConnection, kinds: [any Job.Type], queue: String = "default", logger: Logger? = nil) {
		self.redis = redis
		self.queue = queue
		self.logger = logger
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

	public func perform(_ job: any Job) async {
		do {
			var job = job
			job.logger = self.logger
			try await job.perform()
		} catch {
			let erroredJob = ErroredJob(
				id: UUID().uuidString,
				jobID: job.id,
				error: error.localizedDescription
			)

			do {
				_ = try await redis.lpush(encoder.encode(erroredJob), into: keys.errored).get()
			} catch {
				print("ERROR REPORTING ERROR WOW: \(error)")
				try? await Task.sleep(for: .seconds(1))
			}
		}
	}

	// Queues a job. If a frequency is specified, it's added to the scheduled queue, otherwise it just goes on the main queue.
	// TODO: Maybe look into redis streams?
	public func push(
		_ job: some Job,
		performAt: Date? = nil,
		frequency: JobFrequency = .once
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
				_ = redis.set(keys.remaining(job.id), to: JobScheduler.foreverRemaining)
				_ = redis.set(keys.interval(job.id), to: duration.components.seconds)
			case .once:
				_ = redis.set(keys.remaining(job.id), to: JobScheduler.onceRemaining)
			}
		}
	}

	// Moves scheduled jobs to the main queue. If a job has repeats, it schedules a new one after that job's interval.
	public func schedule(now: Date? = nil) async throws {
		let now = now ?? Date()
		let jobIDsToSchedule = try await redis.zrangebyscore(from: keys.scheduled, withScores: 0...now.timeIntervalSince1970).get().compactMap(\.string)

		logger?.debug("scheduling \(jobIDsToSchedule.count) jobs [Jobsy]")

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

			if remaining == JobScheduler.onceRemaining {
				continue
			}

			guard let interval = try await redis.get(keys.interval(jobID), as: Int.self).get() else {
				throw Error.noIntervalFound
			}

			if remaining == JobScheduler.foreverRemaining {
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

	public func cancel(jobID: String) async throws {
		logger?.debug("canceling \(jobID) jobs [Jobsy]")
		try await transaction {
			_ = redis.delete(keys.all(for: jobID))
			_ = redis.zrem(jobID, from: keys.scheduled)
			_ = redis.lrem(jobID, from: keys.scheduled)
		}
	}

	public func status(jobID: String) async throws -> JobStatus {
		guard let _ = try await redis.get(keys.job(jobID), as: Data.self).get() else {
			return .unknown
		}

		if let remaining = try await redis.get(keys.remaining(jobID), as: Int.self).get(),
			 let timestamp = try await redis.zscore(of: jobID, in: keys.scheduled).get() {
			let interval = try await redis.get(keys.interval(jobID), as: Int.self).get() ?? 0
			let frequency: JobFrequency = if remaining == JobScheduler.onceRemaining {
				.once
			} else if remaining == JobScheduler.foreverRemaining {
				.forever(.seconds(interval))
			} else {
				.times(remaining, .seconds(interval))
			}

			let schedule = JobStatus.Schedule(
				nextPushAt: Date(timeIntervalSince1970: timestamp),
				frequency: frequency
			)

			return .scheduled(schedule)
		} else {
			return .queued
		}
	}

	// Block till a job comes in. This takes a separate redis connection because otherwise no other
	// redis operations will work.
	public func bpop(connection: RedisConnection) async throws -> (any Job)? {
		guard let nextJobID = try await connection.brpop(from: keys.jobs, as: String.self).get() else {
			return nil
		}

		return try await poppedJob(id: nextJobID)
	}

	public func pop() async throws -> (any Job)? {
		guard let nextJobID = try await redis.rpop(from: keys.jobs, as: String.self).get() else {
			return nil
		}

		return try await poppedJob(id: nextJobID)
	}

	func poppedJob(id nextJobID: String) async throws -> (any Job)? {
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

		// TODO: move to a different working queue?
		return try serializedJob.decode(kind, with: decoder)
	}

	@discardableResult private func transaction(block: () -> Void) async throws -> RESPValue {
		_ = redis.send(command: "MULTI", with: [])

		block()

		return try await redis.send(command: "EXEC").get()
	}
}
