//
//  Runner.swift
//
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation
import Logging
@preconcurrency import RediStack

public final class Runner: Sendable {
	let pollInterval: TimeInterval

	public init(pollInterval: TimeInterval) {
		self.pollInterval = pollInterval
	}

	public func run(connection: @autoclosure () -> RedisConnection, for kinds: [any Job.Type], queue: String = "default", logger: Logger = Logger(label: "Jobsy")) async {
		let scheduler = JobScheduler(redis: connection(), kinds: kinds, queue: queue, logger: logger)

		Task.detached {
			while true {
				do {
					try await scheduler.schedule(now: Date())
				} catch {
					print("ERROR SCHEDULING: \(error)")
					try? await Task.sleep(for: .seconds(1))
				}

				try await Task.sleep(for: .seconds(self.pollInterval))
			}

			fatalError("scheduling poller fell through")
		}

		while true {
			do {
				if let job = try await scheduler.bpop(connection: connection()) {
					print("popped job \(job)")
					await scheduler.perform(job)
				}
			} catch {
				print("ERROR POPPING JOB: \(error)")
				try? await Task.sleep(for: .seconds(1))
			}
		}

		fatalError("scheduling bpop fell through")
	}
}
