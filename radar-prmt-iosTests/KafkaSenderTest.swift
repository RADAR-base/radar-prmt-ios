//
//  KafkaSenderTest.swift
//  
//
//  Created by Joris Borgdorff on 15/04/2019.
//

import XCTest
import Swifter
@testable import radar_prmt_ios
import os.log
import BlueSteel

extension HttpServer {
    var url: URL {
        get {
            let host = (try? isIPv4() ? self.listenAddressIPv4 : self.listenAddressIPv6) ?? "localhost"
            return URL(string: "http://" + host + ":" + String(try! port()))!
        }
    }
}

class KafkaSenderTest: XCTestCase {
    var dataController: MockDataController!
    var source: MockSourceManager!
    var sender: KafkaSender!
    var auth: Authorizer!
    var topic: AvroTopic!

    override func setUp() {
        dataController = MockDataController()
        source = MockSourceManager(topicWriter: dataController.writer, sourceId: "a")
        // Put setup code here. This method is called before the invocation of each test method in the class
        auth = MockAuthorizer()
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testDirectSend() throws {
        let expectUpload = expectation(description: "call upload path")
        let server = HttpServer()
        server["/kafka/topics/application_uptime"] = { _ in
            os_log("Did request upload")
            return HttpResponse.ok(HttpResponseBody.json(Dictionary<String, String>() as AnyObject))
        }
        try? server.start()

        let context = MockKafkaSendContext()
        context.successListener = { _ in
            os_log("Upload success")
            expectUpload.fulfill()
        }
        context.failedListener = { _, code, message, _ in
            os_log("Upload failure %d: %@", code, message)
            XCTFail()
        }
        context.serverFailureListener = { _ in
            os_log("Server failure")
            XCTFail()
        }
        context.retryListener = { _ in
            os_log("Should retry")
            XCTFail()
        }

        let encoder = GenericAvroEncoder(encoding: .json)
        var pair = FetchedSchemaPair()
        pair.keySchema = SchemaMetadata(id: 1, version: 1, schema: try Schema.init(json: #"{"name": "ObservationKey", "namespace": "org.radarcns.kafka", "type": "record", "fields": [{"name": "projectId", "type": ["null", "string"]}, {"name": "userId", "type": "string"}, {"name": "sourceId", "type": "string"}]}"#))
        pair.valueSchema = SchemaMetadata(id: 2, version: 1, schema: try Schema.init(json: #"{"name": "ApplicationUptime", "namespace": "org.radarcns.monitor.application", "type": "record", "fields": [{"name": "time", "type": "double"}, {"name": "uptime", "type": "float"}]}"#))
        pair.reset()
        let key = ["projectId": "p", "userId": "u", "sourceId": "s"].toAvro()
        let testFile = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("test")
        FileManager.default.createFile(atPath: testFile.path, contents: nil, attributes: nil)
        let fileHandle = try FileHandle(forWritingTo: testFile)
        let uploadHandle = try JsonUploadHandle(topic: "application_uptime", mediumHandle: FileMediumHandle(file: testFile, fileHandle: fileHandle), schemas: pair, encoder: encoder, key: key)
        try uploadHandle.add(value: ["time": 0.0, "uptime": 1.0])
        try uploadHandle.finalize()

        sender = KafkaSender(baseUrl: server.url, context: context, auth: auth)
        sender.start()
        sender.send(handle: uploadHandle, priority: 1)

        waitForExpectations(timeout: 60, handler: nil)
    }
}
