//
//  GzipRequestMedium.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 25/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import Gzip
import os.log

class GzipRequestMedium: RequestMedium {
    let medium: RequestMedium

    init(medium: RequestMedium) {
        self.medium = medium
    }

    func remove(upload: RecordSetUpload) throws {
        try medium.remove(upload: upload)
    }

    func start(upload: RecordSetUpload) throws -> MediumHandle {
        let handle = try medium.start(upload: upload)
        return try GzipMediumHandle(handle: handle)
    }
}

class GzipMediumHandle: MediumHandle {
    let handle: MediumHandle
    let headers: [String: String]
    var isComplete: Bool {
        return handle.isComplete
    }

    var data: Data? = Data()

    init(handle: MediumHandle) throws {
        self.handle = handle
        self.headers = ["Content-Encoding": "gzip"]
            .merging(handle.headers, uniquingKeysWith: { (_, new) in new })
    }

    func append(data: Data) throws {
        self.data?.append(data)
    }

    func finalize() throws {
        if let data = self.data {
            try handle.append(data: try data.gzipped())
            try handle.finalize()
            // clear memory
            self.data = nil
        }
    }
}
