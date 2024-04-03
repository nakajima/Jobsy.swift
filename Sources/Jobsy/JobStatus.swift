//
//  JobStatus.swift
//
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation

public enum JobStatus: Sendable, Equatable {
	public struct Schedule: Sendable, Equatable {
		public var nextPushAt: Date
		public var frequency: JobFrequency

		#if DEBUG
		public init(nextPushAt: Date, frequency: JobFrequency) {
			self.nextPushAt = nextPushAt
			self.frequency = frequency
		}
		#endif
	}

	case unknown, queued, scheduled(Schedule)
}
