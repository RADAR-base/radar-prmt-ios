//
//  SourceController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import os.log

class SourceController {
    let dataController: DataController
    let sources: [SourceManager]

    init(dataController: DataController) {
        self.dataController = dataController
        self.sources = SourceController.load(dataController: dataController)
    }

    private static func load(dataController: DataController) -> [SourceManager] {
        var sources: [SourceManager] = []
        if let locationManager = LocationManager(topicWriter: dataController.writer, sourceId: "phonesource") {
            sources.append(locationManager)
        } else {
            os_log("Failed to initialize location tracking", type: .error)
        }
        if let spamManager = SpamManager(topicWriter: dataController.writer, sourceId: "phonesource") {
            sources.append(spamManager)
        } else {
            os_log("Failed to initialize spamming", type: .error)
        }
        return sources
    }

    func start() {
        sources.forEach { $0.start() }
    }

    func flush() {
        sources.forEach { $0.flush() }
    }
}
