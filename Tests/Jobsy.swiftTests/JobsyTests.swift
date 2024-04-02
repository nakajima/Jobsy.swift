import Jobsy
import XCTest
import Foundation
import pat_swift
@preconcurrency import RediStack

actor TestThing {
	static let instance = TestThing()

	var count = 0

	private init() { }

	func reset() { count = 0 }
	func increment() { count += 1 }
}

struct TestJob: Job {
	struct Parameters: Codable {
		var name: String = "test job"
		var shouldError = false
	}

	enum Error: Swift.Error {
		case testError
	}

	var id = UUID().uuidString
	var frequency: JobFrequency = .once
	var parameters: Parameters = Parameters()

	init(id: String, parameters: Parameters) {
		self.id = id
		self.parameters = parameters
	}

	func perform() async throws {
		if parameters.shouldError {
			throw Error.testError
		} else {
			await TestThing.instance.increment()
		}
	}
}

final class Jobsy_swiftTests: XCTestCase {
	let scheduler = JobScheduler(redis: .dev, kinds: [TestJob.self])

	func reset() async throws {
		let redis: RedisConnection = .dev

		let keys = try await redis.send(command: "KEYS", with: ["jobsy:*".convertedToRESPValue()]).get().array ?? []
		_ = try await redis.delete(keys.compactMap({ $0.string }).map { RedisKey($0) }).get()

		await TestThing.instance.reset()
	}

	func testCanConnect() throws {
		let redis = Redis(connection: .dev)
		XCTAssert(redis.isConnected, "could not connect, is the local server running?")
	}

	func testEnqueueAndPerform() async throws {
		try await reset()

		let testJob = TestJob(id: "sup", parameters: .init(name: "test job"))
		try await scheduler.push(testJob)

		let job = try await XCTUnwrapAsync(await scheduler.pop())

		XCTAssertEqual(testJob.id, job.id)
		try await job.perform()

		let newCount = await TestThing.instance.count
		XCTAssertEqual(newCount, 1)

		let nextJob = try await scheduler.pop()
		XCTAssertNil(nextJob)
	}

	func testCanPerformLater() async throws {
		try await reset()

		let scheduledJob = TestJob(id: "sup", parameters: .init(name: "scheduled job"))

		try await scheduler.push(scheduledJob, performAt: Date().addingTimeInterval(2))
		try await scheduler.schedule()

		let job = try await scheduler.pop()

		XCTAssertNil(job, "popped job before perform at")

		try? await Task.sleep(for: .seconds(2.5))
		try await scheduler.schedule()

		let foundJob = try await XCTUnwrapAsync(await scheduler.pop())
		XCTAssertEqual(foundJob.id, scheduledJob.id)
	}

	func testCanRepeat() async throws {
		try await reset()

		let scheduledJob = TestJob(id: "repeating", parameters: .init(name: "repeating job"))

		var date = Date()
		try await scheduler.push(scheduledJob, performAt: date.addingTimeInterval(1), frequency: .times(3, .seconds(1)))
		try await scheduler.schedule(now: date)

		let job = try await scheduler.pop()

		XCTAssertNil(job, "popped job before perform at")

		// We should get our first one
		date = date.advanced(by: 1.1)
		try await scheduler.schedule(now: date)

		let found1 = try await XCTUnwrapAsync(await scheduler.pop())
		XCTAssertEqual(found1.id, scheduledJob.id)

		let found1next = try await scheduler.pop()
		XCTAssertNil(found1next)

		// We should get our second one
		date = date.advanced(by: 1)
		try await scheduler.schedule(now: date)

		let found2 = try await XCTUnwrapAsync(await scheduler.pop())
		XCTAssertEqual(found2.id, scheduledJob.id)

		let found2next = try await scheduler.pop()
		XCTAssertNil(found2next)

		// We should get our third one
		date = date.advanced(by: 1)
		try await scheduler.schedule(now: date)

		let found3 = try await XCTUnwrapAsync(await scheduler.pop())
		XCTAssertEqual(found3.id, scheduledJob.id)

		let found3next = try await scheduler.pop()
		XCTAssertNil(found3next)

		// We shouldn't have a fourth one
		date = date.advanced(by: 1)
		try await scheduler.schedule(now: date)

		let found4 = try await scheduler.pop()
		XCTAssertNil(found4, "should not have another one")
	}

	func testErroredJob() async throws {
		try await reset()

		let job = TestJob(id: "errored", parameters: .init(shouldError: true))

		try await scheduler.perform(job)

		let errored = try await scheduler.errored()

		XCTAssertEqual([job.id], errored.map(\.jobID))
	}
}
