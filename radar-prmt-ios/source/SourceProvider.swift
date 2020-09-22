//
//  SourceProvider.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation

protocol SourceProvider {
    var defaultSourceType: SourceType { get }
    var pluginDefinition: PluginDefinition { get }
    func update(state: RadarState)
    func provide(sourceManager: SourceManager) -> SourceProtocol?
}

extension SourceProvider {
    func matches(sourceType: SourceType) -> Bool {
        return sourceType.producer.caseInsensitiveCompare(defaultSourceType.producer) == .orderedSame
            && sourceType.model.caseInsensitiveCompare(defaultSourceType.model) == .orderedSame
    }
}
