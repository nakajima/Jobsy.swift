//
//  Runner.swift
//
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation
@preconcurrency import RediStack

public final class Runner: Sendable {
	let pollInterval: TimeInterval

	public init(pollInterval: TimeInterval) {
		self.pollInterval = pollInterval
	}

	public func run(connection: @autoclosure () -> RedisConnection, for kinds: [any Job.Type]) async {
		let scheduler = JobScheduler(redis: connection(), kinds: kinds)

		Task {
			while true {
				do {
					try await scheduler.schedule(now: Date())
				} catch {
					print("ERROR SCHEDULING: \(error)")
					try? await Task.sleep(for: .seconds(1))
				}

				try await Task.sleep(for: .seconds(self.pollInterval))
			}
		}

		while true {
			do {
				if let job = try await scheduler.bpop(connection: connection()) {
					await scheduler.perform(job)
				}
			} catch {
				print("ERROR POPPING JOB: \(error)")
				try? await Task.sleep(for: .seconds(1))
			}
		}
	}
}
