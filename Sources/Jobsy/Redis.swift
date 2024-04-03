//
//  Redis.swift
//
//
//  Created by Pat Nakajima on 4/1/24.
//

import RediStack
import NIOCore
import Foundation

public extension RedisConnection {
	static func dev() -> RedisConnection {
		let eventLoop: EventLoop = NIOSingletons.posixEventLoopGroup.any()

		do {
			return try RedisConnection.make(
				configuration: .init(hostname: "127.0.0.1"),
				boundEventLoop: eventLoop
			).wait()
		} catch {
			fatalError("error connecting to dev redis: \(error)")
		}
	}

	static func url(_ url: String) -> RedisConnection {
		let eventLoop: EventLoop = NIOSingletons.posixEventLoopGroup.any()

		guard let url = URL(string: url) else {
			fatalError("invalid redis connection URL")
		}

		do {
			return try RedisConnection.make(
				configuration: .init(url: url),
				boundEventLoop: eventLoop
			).wait()
		} catch {
			fatalError("error connecting to redis url: \(error)")
		}
	}
}

