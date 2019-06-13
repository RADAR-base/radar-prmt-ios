//
//  SchemaRegistryClientTest.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 15/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import XCTest
import RxSwift
@testable import radar_prmt_ios

class SchemaRegistryClientTest: XCTestCase {
    var client: SchemaRegistryClient!
    var disposeBag: DisposeBag!

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
        client = SchemaRegistryClient(baseUrl: URL(string: "https://radar-test.thehyve.net")!)
        disposeBag = DisposeBag()
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
        let expectCallback = expectation(description: "should read schema from server")

        client.requestSchemas(for: "application_record_counts")
            .subscribe(onNext: { pair in
                guard case let .success((keySchema, valueSchema)) = pair else { XCTFail(); return }
                XCTAssertEqual("org.radarcns.kafka.ObservationKey", keySchema.schema.typeName)
                XCTAssertEqual("org.radarcns.monitor.application.ApplicationRecordCounts", valueSchema.schema.typeName)
                expectCallback.fulfill()
            }, onError: { error in
                XCTFail()
            }).disposed(by: disposeBag)

        waitForExpectations(timeout: 5, handler: nil)

        let expectCacheCallback = expectation(description: "should read schema from cache")

        client.requestSchemas(for: "application_record_counts")
            .subscribe(onNext: { pair in
                guard case let .success((keySchema, valueSchema)) = pair else { XCTFail(); return }
                XCTAssertEqual("org.radarcns.kafka.ObservationKey", keySchema.schema.typeName)
                XCTAssertEqual("org.radarcns.monitor.application.ApplicationRecordCounts", valueSchema.schema.typeName)
                expectCacheCallback.fulfill()
            }, onError: { _ in XCTFail() })
            .disposed(by: disposeBag)

        waitForExpectations(timeout: 0.01, handler: nil)
    }

    func testRequestNonExistingSchema() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        let expectCallback = expectation(description: "should read schema from server")

        client.requestSchemas(for: "does_not_exist")
            .subscribe(onNext: { _ in XCTFail() },
                       onError: { _ in expectCallback.fulfill() })
            .disposed(by: disposeBag)

        waitForExpectations(timeout: 5, handler: nil)
    }
}
