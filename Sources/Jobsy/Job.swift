//
//  Job.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

import Foundation
@preconcurrency import RediStack

public protocol Job: Codable, Sendable {
	associatedtype Parameters: Codable

	var id: String { get }
	var parameters: Parameters { get }

	init(id: String, parameters: Parameters)

	func perform() async throws
}

public extension Job {
	var kind: String { String(describing: Self.self) }
}
