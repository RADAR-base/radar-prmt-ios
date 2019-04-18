//
//  SchemaRegistryClientTest.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 15/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import XCTest
@testable import radar_prmt_ios

class SchemaRegistryClientTest: XCTestCase {
    var client: SchemaRegistryClient? = nil

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
        client = SchemaRegistryClient(baseUrl: URL(string: "https://radar-test.thehyve.net")!)
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testProperties() {
        XCTAssertEqual(URL(string: "https://radar-test.thehyve.net/schema/subjects/"), client?.schemaUrl)
    }

    func testRequestSchema() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        guard let client = self.client else { return }

        let expectCallback = expectation(description: "should read schema from server")

        client.requestSchemas(for: "application_record_counts") { pair in
            XCTAssert(pair.isComplete)
            XCTAssert(pair.isUpToDate)
            XCTAssertEqual("org.radarcns.kafka.ObservationKey", pair.keySchema!.schema.typeName)
            XCTAssertEqual("org.radarcns.monitor.application.ApplicationRecordCounts", pair.valueSchema!.schema.typeName)
            expectCallback.fulfill()
        }
        waitForExpectations(timeout: 5, handler: nil)

        let expectCacheCallback = expectation(description: "should read schema from cache")

        client.requestSchemas(for: "application_record_counts") { pair in
            XCTAssert(pair.isComplete)
            XCTAssertEqual("org.radarcns.kafka.ObservationKey", pair.keySchema!.schema.typeName)
            XCTAssertEqual("org.radarcns.monitor.application.ApplicationRecordCounts", pair.valueSchema!.schema.typeName)
            expectCacheCallback.fulfill()
        }
        waitForExpectations(timeout: 0.01, handler: nil)
    }

    func testRequestNonExistingSchema() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        guard let client = self.client else { return }

        let expectCallback = expectation(description: "should read schema from server")

        client.requestSchemas(for: "does_not_exist") { pair in
            XCTAssert(!pair.isComplete)
            XCTAssert(pair.isUpToDate)
            expectCallback.fulfill()
        }
        waitForExpectations(timeout: 5, handler: nil)
    }
}
