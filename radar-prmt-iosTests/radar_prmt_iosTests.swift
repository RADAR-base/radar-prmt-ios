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
import RxSwift
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

        let disposeBag = DisposeBag()

        reader.nextInQueue(minimumPriority: 0)
            .subscribe(onNext: { element in
                switch element {
                case .fresh(topic: "test", dataGroupId: _):
                    expectCallback.fulfill()
                default:
                    XCTFail()
                }
            }, onError: { _ in XCTFail() })
            .disposed(by: disposeBag)

        waitForExpectations(timeout: 1, handler: nil)
    }



    func testDataInQueue() {
        insertData()

        let expectCallback = expectation(description: "should read next topic")
        let reader = dataController.reader

        let disposeBag = DisposeBag()

        let schemaPair = (
            SchemaMetadata(id: 1, version: 1, schema: try! Schema.init(json: #"{"name": "ObservationKey", "namespace": "org.radarcns.kafka", "type": "record", "fields": [{"name": "projectId", "type": ["null", "string"]}, {"name": "userId", "type": "string"}, {"name": "sourceId", "type": "string"}]}"#)),
            SchemaMetadata(id: 2, version: 1, schema: topic.valueSchema)
        )

        let auth = MockAuthorizer()
        let medium = DataRequestMedium()
        let context = JsonUploadContext(auth: auth, medium: medium)

        reader.nextInQueue(minimumPriority: 0)
            .flatMap { element in
                reader.prepareUpload(for: element, with: context, schemas: schemaPair)
            }
            .subscribe(onNext: { handleAndMore in
                let (handle, hasMore) = handleAndMore
                XCTAssertFalse(hasMore)
                XCTAssertTrue(handle.mediumHandle.isComplete)
                guard let mediumHandle = handle.mediumHandle as? DataMediumHandle else { XCTFail(); return }
                XCTAssertEqual(83, mediumHandle.data.count)
                expectCallback.fulfill()
            }, onError: { _ in XCTFail() })
            .disposed(by: disposeBag)

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
}
