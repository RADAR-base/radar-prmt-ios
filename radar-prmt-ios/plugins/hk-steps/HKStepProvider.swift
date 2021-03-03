//
//  HKStepProvider.swift
//  radar-prmt-ios
//
//  Created by Peyman Mohtashami on 02/03/2021.
//  Copyright © 2021 Joris Borgdorff. All rights reserved.
//

import Foundation

class HKStepProvider : SourceProvider {
    var pluginDefinition = PluginDefinition(pluginNames: ["hk_step", "ios_hk_step", "HKStepProvider"], supportsBackground: true)
    let defaultSourceType = SourceType(id: 40901, producer: "HEALTHKIT", model: "Step", version: Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "0.0.1", canRegisterDynamically: true)

    func update(state: RadarState) {
        pluginDefinition.supportsBackground = Bool(state.config["ios_hk_step_background_enabled",  default: "true"]) ?? true
    }

    func provide(sourceManager: SourceManager) -> SourceProtocol? {
        return HKStepProtocol(manager: sourceManager)
    }
}
