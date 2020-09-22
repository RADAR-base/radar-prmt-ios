//
//  SourceProtocol.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import RxSwift

protocol SourceProtocol {
    func startScanning() -> Single<Source>
    func registerTopics() -> Bool
    func startCollecting()
    func close()
    func closeForeground()
}
