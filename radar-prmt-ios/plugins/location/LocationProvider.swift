//
//  LocationProvider.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 13/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

class LocationProvider : SourceProvider {
    var sourceDefinition = SourceDefinition(pluginNames: ["location", "ios_location", "LocationProvider"], supportsBackground: true)

    func provide(writer: AvroDataWriter, authConfig: RadarState) -> SourceManager? {
        return LocationManager(provider: DelegatedSourceProvider(self), topicWriter: writer, sourceId: "041220d6-dc14-47b3-acf2-670d25dcdb93")
    }

    func matches(sourceType: SourceType) -> Bool {
        return sourceType.producer.caseInsensitiveCompare("Apple") == .orderedSame
            && sourceType.model.caseInsensitiveCompare("iOS") == .orderedSame
    }
}
