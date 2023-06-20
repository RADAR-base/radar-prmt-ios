//
//  SpamManager.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxSwift

class SpamProtocol: SourceProtocol {
    let queue = DispatchQueue(label: "spammer", qos: .userInitiated)
    private var queueIsSuspended = false
    var spamTopic: AvroTopicCacheContext!
    let manager: SourceManager

    init(sourceManager: SourceManager) {
        self.manager = sourceManager
    }

    func startScanning() -> Single<Source> {
        if let source = (manager.findSource { _ in true}) {
            return manager.use(source: source)
        } else {
            return manager.use(source: Source(type: manager.sourceType, id: nil, name: "spam", expectedName: nil, attributes: nil))
        }
    }

    func startCollecting() {
        if queueIsSuspended {
            queue.resume()
            queueIsSuspended = false
        }
        createSpam()
    }

    func registerTopics() -> Bool {
        if let topic = manager.define(topic: "spam", valueSchemaPath: "passive/phone/phone_acceleration") {
            spamTopic = topic
            return true
        } else {
            return false
        }
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

    func closeForeground() {
        if !queueIsSuspended {
            queue.suspend()
            queueIsSuspended = true
        }
    }

    func close() {}
}
