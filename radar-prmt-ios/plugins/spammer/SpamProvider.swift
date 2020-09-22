//
//  SpamProvider.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 13/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

class SpamProvider : SourceProvider {
    let defaultSourceType: SourceType = SourceType(id: 2, producer: "Apple_ios", model: "spam", version: "1.0.0", canRegisterDynamically: false)
    let pluginDefinition = PluginDefinition(pluginNames: ["spam", "ios_spam", "SpamProvider"])

    func update(state: RadarState) {}

    func provide(sourceManager: SourceManager) -> SourceProtocol? {
        return SpamProtocol(sourceManager: sourceManager)
    }

    func matches(sourceType: SourceType) -> Bool {
        return sourceType.producer.caseInsensitiveCompare("Apple_ios") == .orderedSame
            && sourceType.model.caseInsensitiveCompare("spam") == .orderedSame
    }
}
