//
//  SpamProvider.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 13/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

class SpamProvider : SourceProvider {
    let sourceDefinition = SourceDefinition(pluginNames: ["spam", "ios_spam", "SpamProvider"])

    func provide(writer: AvroDataWriter, authConfig: RadarState) -> SourceManager? {
        return SpamManager(provider: DelegatedSourceProvider(self), topicWriter: writer, sourceId: "phonesource")
    }

    func matches(sourceType: SourceType) -> Bool {
        return sourceType.producer.caseInsensitiveCompare("Apple_ios") == .orderedSame
            && sourceType.model.caseInsensitiveCompare("spam") == .orderedSame
    }
}
