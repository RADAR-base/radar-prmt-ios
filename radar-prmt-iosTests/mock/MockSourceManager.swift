//
//  MockSourceManager.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 15/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
@testable import radar_prmt_ios

class MockSourceManager : SourceManager {
    var topic: AvroTopicCacheContext!
    let start = Date()

    override init?(topicWriter writer: AvroDataWriter, sourceId: String) {
        super.init(topicWriter: writer, sourceId: sourceId)

        if let locTopic = define(topic: "application_uptime", valueSchemaPath: "monitor/application/application_uptime") {
            topic = locTopic
        } else {
            return nil
        }
    }

    func newRecord() -> AvroValue {
        return try! AvroValue(value: [
            "time": Date().timeIntervalSince1970,
            "uptime": start.timeIntervalSinceNow,
            ], as: topic.topic.valueSchema)
    }

    func storeRecord() {
        topic.add(record: newRecord())
        topic.flush()
    }
}
