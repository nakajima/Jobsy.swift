//
//  RedisKeys.swift
//  
//
//  Created by Pat Nakajima on 4/2/24.
//

import RediStack

struct RedisKeys {
	var queue: String

	func job(_ id: String) -> RedisKey {
		RedisKey("\(prefix):job:\(id)")
	}

	var jobs: RedisKey {
		RedisKey("\(prefix):jobs")
	}

	var scheduled: RedisKey {
		RedisKey("\(prefix):scheduled")
	}

	func remaining(_ jobID: String) -> RedisKey {
		RedisKey("\(job(jobID)):remaining")
	}

	func interval(_ jobID: String) -> RedisKey {
		RedisKey("\(job(jobID)):interval")
	}

	var errored: RedisKey {
		RedisKey("\(prefix):errored")
	}

	var prefix: RedisKey {
		"jobsy:\(queue)"
	}

	func all(for jobID: String) -> [RedisKey] {
		[
			job(jobID),
			remaining(jobID),
			interval(jobID)
		]
	}
}
