//
//  JobFrequency.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

import Foundation

public enum JobFrequency: Codable, Sendable, Equatable {
	case once, times(Int, Duration), forever(Duration)
}
