//
//  Redis.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

@preconcurrency import RediStack
import NIOCore

public extension RedisConnection {
	static let dev: RedisConnection = {
		let eventLoop: EventLoop = NIOSingletons.posixEventLoopGroup.any()

		return try! RedisConnection.make(
			configuration: .init(hostname: "127.0.0.1"),
			boundEventLoop: eventLoop
		).wait()
	}()
}

public final class Redis {
	let connection: RedisConnection

	public init(connection: RedisConnection) {
		self.connection = connection
	}

	public var isConnected: Bool {
		self.connection.isConnected
	}
}
