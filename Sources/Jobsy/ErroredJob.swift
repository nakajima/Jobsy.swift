//
//  ErroredJob.swift
//
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation

public struct ErroredJob: Codable, Sendable {
	public let id: String
	public let jobID: String
	public let error: String
}
