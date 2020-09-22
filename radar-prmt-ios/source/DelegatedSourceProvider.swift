//
//  DelegatedSourceProvider.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

struct DelegatedSourceProvider: SourceProvider, Equatable, Hashable {
    let delegate: SourceProvider

    var pluginDefinition: PluginDefinition { return delegate.pluginDefinition }
    var defaultSourceType: SourceType { return delegate.defaultSourceType}

    func provide(sourceManager: SourceManager) -> SourceProtocol? {
        return delegate.provide(sourceManager: sourceManager)
    }
    func matches(sourceType: SourceType) -> Bool {
        return delegate.matches(sourceType: sourceType)
    }
    func update(state: RadarState) {
        self.delegate.update(state: state)
    }
    init(_ provider: SourceProvider) {
        self.delegate = provider
    }

    static func ==(lhs: DelegatedSourceProvider, rhs: DelegatedSourceProvider) -> Bool {
        return lhs.delegate.pluginDefinition.pluginName == rhs.delegate.pluginDefinition.pluginName
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(delegate.pluginDefinition.pluginName)
    }
}
