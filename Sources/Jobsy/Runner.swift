//
//  Runner.swift
//
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation
@preconcurrency import RediStack

public final class Runner: Sendable {
	let scheduler: JobScheduler
	let pollInterval: TimeInterval

	public init(scheduler: JobScheduler, pollInterval: TimeInterval) {
		self.scheduler = scheduler
		self.pollInterval = pollInterval
	}

	public func run() async throws {
		Task {
			while true {
				do {
					try await self.scheduler.schedule(now: Date())
				} catch {
					print("ERROR SCHEDULING: \(error)")
					try? await Task.sleep(for: .seconds(1))
				}

				try await Task.sleep(for: .seconds(self.pollInterval))
			}
		}

		while true {
			if let job = try await scheduler.bpop(connection: .dev()) {
				try await scheduler.perform(job)
			}
		}
	}
}
