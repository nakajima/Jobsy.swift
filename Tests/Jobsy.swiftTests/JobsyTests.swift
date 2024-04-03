import Jobsy
import XCTest
import Foundation
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

public extension XCTestCase {
	func XCTUnwrapAsync<T: Sendable>(_ expression: @autoclosure () async throws -> T?, _ message: @autoclosure () -> String = "", file: StaticString = #filePath, line: UInt = #line) async throws -> T {

		let result = try await expression()

		return try await MainActor.run {
			return try XCTUnwrap(result)
		}
	}
}

final class Jobsy_swiftTests: XCTestCase {
	let scheduler = JobScheduler(redis: .dev(), kinds: [TestJob.self])

	func reset() async throws {
		let redis: RedisConnection = .dev()

		let keys = try await redis.send(command: "KEYS", with: ["jobsy:*".convertedToRESPValue()]).get().array ?? []
		_ = try await redis.delete(keys.compactMap({ $0.string }).map { RedisKey($0) }).get()

		await TestThing.instance.reset()
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

		let date = Date()

		try await scheduler.push(scheduledJob, performAt: date.addingTimeInterval(2))
		try await scheduler.schedule(now: date)

		let job = try await scheduler.pop()

		XCTAssertNil(job, "popped job before perform at")

		try await scheduler.schedule(now: date.addingTimeInterval(2.5))

		let foundJob = try await XCTUnwrapAsync(await scheduler.pop())
		XCTAssertEqual(foundJob.id, scheduledJob.id)

		let nojob = try await scheduler.pop()

		XCTAssertNil(nojob, "popped job after there shouldnt be any left")
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

	func testStatus() async throws {
		try await reset()

		let normalJob = TestJob(id: "normal", parameters: .init(shouldError: false))

		var status = try await scheduler.status(jobID: "normal")
		XCTAssertEqual(status, .unknown)

		try await scheduler.push(normalJob)

		status = try await scheduler.status(jobID: "normal")
		XCTAssertEqual(status, .queued)

		let scheduledJob = TestJob(id: "scheduled", parameters: .init())

		status = try await scheduler.status(jobID: "scheduled")
		XCTAssertEqual(status, .unknown)

		let performAt = Date().addingTimeInterval(10)
		try await scheduler.push(scheduledJob, performAt: performAt)

		let scheduledStatus = try await scheduler.status(jobID: "scheduled")
		if case let .scheduled(schedule) = scheduledStatus {
			XCTAssertEqual(schedule.nextPushAt.description, performAt.description)
			XCTAssertEqual(schedule.frequency, .once)
		} else {
			XCTFail("did not get scheduled: \(scheduledStatus)")
		}
	}

	func testRunner() async throws {
		try await reset()

		let job = TestJob(id: "job", parameters: .init())
		let scheduledJob = TestJob(id: "scheduled", parameters: .init())

		try await scheduler.push(job)
		try await scheduler.push(scheduledJob, performAt: Date().addingTimeInterval(2))

		let runner = Runner(pollInterval: 1)
		
		Task {
			try await runner.run(connection: .dev(), for: [TestJob.self])
		}

		for _ in 0..<4 {
			try? await Task.sleep(for: .seconds(1))

			let newCount = await TestThing.instance.count
			if newCount == 2 {
				XCTAssert(true)
				return
			}
		}

		XCTFail("did not get to 2")
	}
}
