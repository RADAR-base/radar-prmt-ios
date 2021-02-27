//
//  LocationProvider.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 13/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

class LocationProvider : SourceProvider {
    var pluginDefinition = PluginDefinition(pluginNames: ["location", "ios_location", "LocationProvider"], supportsBackground: true)
    let defaultSourceType = SourceType(id: 20901, producer: "Apple", model: "iOS", version: Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "0.0.1", canRegisterDynamically: true)

    func update(state: RadarState) {
        pluginDefinition.supportsBackground = Bool(state.config["ios_location_background_enabled",  default: "true"]) ?? true
    }

    func provide(sourceManager: SourceManager) -> SourceProtocol? {
        return LocationProtocol(manager: sourceManager)
    }
}
