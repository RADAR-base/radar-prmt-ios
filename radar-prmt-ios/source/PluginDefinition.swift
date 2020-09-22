//
//  PluginDefinition.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

struct PluginDefinition {
    let pluginName: String
    let pluginNames: [String]
    var supportsBackground: Bool

    init(pluginNames: [String], supportsBackground: Bool = false) {
        self.pluginName = pluginNames[0]
        self.pluginNames = pluginNames
        self.supportsBackground = supportsBackground
    }
}

extension PluginDefinition: Equatable {
    static func == (lhs: PluginDefinition, rhs: PluginDefinition) -> Bool {
        return lhs.pluginName == rhs.pluginName
    }
}
