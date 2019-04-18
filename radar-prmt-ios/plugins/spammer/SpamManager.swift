//
//  SpamManager.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

class SpamManager : SourceManager {
    let queue: DispatchQueue
    var spamTopic: AvroTopicCacheContext!

    override init?(topicWriter: AvroDataWriter, sourceId: String) {
        queue = DispatchQueue(label: "spammer", qos: .userInitiated)
        super.init(topicWriter: topicWriter, sourceId: sourceId)
        if let locTopic = define(topic: "spam", valueSchemaPath: "passive/phone/phone_acceleration") {
            spamTopic = locTopic
        } else {
            return nil
        }
    }

    override func start() {
        createSpam()
    }

    func createSpam() {
        queue.asyncAfter(deadline: .now() + 0.001) { [weak self] in
            guard let self = self else { return }
            self.spamTopic.add(record: [
                "time": Date().timeIntervalSince1970,
                "timeReceived": Date().timeIntervalSince1970,
                "x": 0.1,
                "y": -0.1,
                "z": 0.0
                ])
            self.createSpam()
        }
    }
}
