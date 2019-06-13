//
//  RequestMedium.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 18/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import os.log

enum RequestMediumError: Error {
    case notStarted
    case alreadyStarted
    case encodingError
}

protocol RequestMedium {
    func remove(upload: RecordSetUpload) throws
    func start(upload: RecordSetUpload) throws -> MediumHandle
}

protocol MediumHandle {
    var headers: [String: String] { get }
    var isComplete: Bool { get }
    func append(data: Data) throws
    func finalize() throws
}

extension MediumHandle {
    func append(string: String) throws {
        guard let data = string.data(using: .utf8) else {
            throw RequestMediumError.encodingError
        }
        try append(data: data)
    }
}

class FileRequestMedium: RequestMedium {
    private let writeDirectory: URL

    init(writeDirectory: URL) {
        self.writeDirectory = writeDirectory
    }

    func remove(upload: RecordSetUpload) throws {
        if let file = upload.file {
            do {
                try FileManager.default.removeItem(at: file)
                os_log("Removed old upload file %@", type: .debug, file.path)
            } catch {
                os_log("Old upload file %@ cannot be removed: %@", file.path, error.localizedDescription)
            }
        }
    }

    func start(upload: RecordSetUpload) throws -> MediumHandle {
        let fileMgr = FileManager.default
        if let file = upload.file,
                fileMgr.isReadableFile(atPath: file.path),
                let fileAttr = try? fileMgr.attributesOfItem(atPath: file.path),
                let size = fileAttr[.size] as? NSNumber,
                size.int64Value > 0 {
            return FileMediumHandle(file: file, fileHandle: nil)
        } else {
            let file = writeDirectory.appendingPathComponent(upload.topic!.name!)
            upload.file = file
            fileMgr.createFile(atPath: file.path, contents: nil, attributes: nil)
            let fileHandle = try FileHandle(forWritingTo: file)
            return FileMediumHandle(file: file, fileHandle: fileHandle)
        }
    }
}

class FileMediumHandle: MediumHandle {
    let headers: [String : String] = [:]
    let fileHandle: FileHandle?
    let file: URL
    var isComplete: Bool

    init(file: URL, fileHandle: FileHandle?) {
        self.file = file
        self.fileHandle = fileHandle
        isComplete = fileHandle == nil
    }

    func append(data: Data) throws {
        self.fileHandle?.write(data)
    }

    func finalize() throws {
        self.fileHandle?.closeFile()
        isComplete = true
    }
}

struct DataRequestMedium: RequestMedium {
    func remove(upload: RecordSetUpload) throws {
    }

    func start(upload: RecordSetUpload) throws -> MediumHandle {
        return DataMediumHandle()
    }
}

class DataMediumHandle: MediumHandle {
    let headers: [String : String] = [:]
    var isComplete: Bool = false
    var data: Data = Data()

    func append(data: Data) throws {
        assert(!isComplete)
        self.data.append(data)
    }

    func finalize() throws {
        assert(!isComplete)
        isComplete = true
    }
}
