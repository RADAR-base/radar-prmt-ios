//
//  radar_prmt_iosTests.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 10/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import XCTest
import CoreData
import BlueSteel
import os.log
@testable import radar_prmt_ios

class radar_prmt_iosTests: XCTestCase {
    var dataController: MockDataController!
    var topic = try! AvroTopic(name: "test", valueSchema: "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")

    override func setUp() {
        dataController = MockDataController()
        topic.priority = 1
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    private func insertData() {
        let writer = dataController.writer
        let dataGroupId = writer.register(topic: topic, sourceId: "s")!
        let writeContext = AvroTopicCacheContext(topic: topic, dataGroup: dataGroupId, queue: DispatchQueue(label: "test"), encoder: GenericAvroEncoder(encoding: .binary), topicWriter: writer)
        writeContext.add(record: ["a": 1])
        writeContext.flush()
        usleep(50000)
    }

    func testWrite() {
        insertData()

        let moc: NSManagedObjectContext = dataController.container.newBackgroundContext()
        moc.performAndWait {
            let request: NSFetchRequest<RecordSet> = RecordSet.fetchRequest()
            let records = try! moc.fetch(request)
            XCTAssertEqual(1, records.count)
            for record in records {
                XCTAssert(record.dataContainer != nil)
                XCTAssert(record.group != nil)
                XCTAssert(record.time != nil)
                XCTAssert(record.uploadPart == nil)
                XCTAssert(record.topic != nil)
                if let topic = record.topic {
                    XCTAssert(topic.upload == nil)
                    XCTAssertEqual("test", topic.name)
                    XCTAssertEqual(1, topic.priority)
                }
            }
        }
    }

    func testFetchRelated() {
        insertData()

        let moc: NSManagedObjectContext = dataController.container.newBackgroundContext()
        moc.performAndWait {
            var request: NSFetchRequest<RecordSet> = RecordSet.fetchRequest()
            request.predicate = NSPredicate(format: "topic.upload == NULL AND topic.priority >= %d", 0)
            var records = try! moc.fetch(request)
            XCTAssertEqual(1, records.count)
            guard let dataGroup = records.first?.group else { return }

            request = RecordSet.fetchRequest()
            request.predicate = NSPredicate(format: "group == %@", dataGroup)
            records = try! moc.fetch(request)
            XCTAssertEqual(1, records.count)
        }
    }

    func testNextInQueue() {
        insertData()

        let expectCallback = expectation(description: "should read next topic")
        let reader = dataController.reader

        reader.nextInQueue(minimumPriority: 0) { topic, dataGroupId in
            XCTAssert(topic != nil)
            XCTAssert(dataGroupId != nil)
            if let topic = topic {
                XCTAssertEqual("test", topic)
            }
            expectCallback.fulfill()
        }

        waitForExpectations(timeout: 1, handler: nil)
    }
//
//    func testRead() {
//
//        let expectCallback = expectation(description: "should read records")
//        let reader = dataController.reader
//
//        reader.readNextRecords(minimumPriority: 0) { [weak self] value in
//            guard let self = self else { return }
//            XCTAssert(value != nil)
//            if let value = value {
//                XCTAssertEqual(0, value.priority)
//                XCTAssertEqual("s", value.sourceId)
//                XCTAssertEqual("test", value.topic)
//                XCTAssertEqual([try! AvroValue.init(value: ["a": 1], as: self.topic.valueSchema)], value.values)
//            }
//        }
//
//    }

    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }

}
