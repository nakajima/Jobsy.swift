//
//  Job.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

import Foundation
import RediStack
import Logging

public protocol Job: Sendable {
	associatedtype Parameters: Codable

	var id: String { get }
	var parameters: Parameters { get }
	var logger: Logger { get set }

	init(id: String, parameters: Parameters)

	func perform() async throws
}

public extension Job {
	var kind: String { String(describing: Self.self) }
}
