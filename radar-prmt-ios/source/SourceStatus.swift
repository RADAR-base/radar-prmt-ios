//
//  SourceStatus.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 19/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

enum SourceStatus {
    case initializing
    case scanning
    case connecting
    case waitingForUserInput
    case collecting
    case disconnecting
    case disconnected
    case invalid
}
