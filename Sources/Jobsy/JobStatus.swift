//
//  JobStatus.swift
//
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation

public enum JobStatus: Sendable {
	public struct Schedule: Sendable {
		public var nextPushAt: Date
		public var interval: TimeInterval
		public var remaining: Int?
	}

	case unknown, queued, scheduled(Schedule)
}
