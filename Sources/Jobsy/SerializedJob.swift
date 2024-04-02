//
//  SerializedJob.swift
//  
//
//  Created by Pat Nakajima on 4/2/24.
//

import Foundation

struct SerializedJob: Codable {
	var id: String
	let kind: String
	let parameters: Data

	init(id: String, kind: String, parameters: Data) {
		self.id = id
		self.kind = kind
		self.parameters = parameters
	}

	func decode<T: Job>(_ type: T.Type, with decoder: JSONDecoder) throws -> T {
		let parameters = try decoder.decode(T.Parameters.self, from: parameters)
		return T(id: id, parameters: parameters)
	}
}
