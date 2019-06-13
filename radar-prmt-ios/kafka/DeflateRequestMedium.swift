//
//  GzipRequestMedium.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 25/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import Compression
import os.log

extension compression_stream {
    init() {
        self = UnsafeMutablePointer<compression_stream>.allocate(capacity: 1).pointee
    }
}

class DeflateRequestMedium: RequestMedium {
    let medium: RequestMedium
    let algorithm: compression_algorithm
    let encoding: String

    init(medium: RequestMedium, algorithm: compression_algorithm = COMPRESSION_ZLIB, encoding: String = "deflate") {
        self.medium = medium
        self.algorithm = algorithm
        self.encoding = encoding
    }

    func remove(upload: RecordSetUpload) throws {
        try medium.remove(upload: upload)
    }

    func start(upload: RecordSetUpload) throws -> MediumHandle {
        let handle = try medium.start(upload: upload)
        return try DeflateMediumHandle(handle: handle, algorithm: algorithm, encoding: encoding)
    }
}

class DeflateMediumHandle: MediumHandle {
    let handle: MediumHandle
    let headers: [String : String]
    var isComplete: Bool {
        get {
            return handle.isComplete
        }
    }

    var stream: compression_stream = compression_stream()
    let bufferSize = 32_768
    let destinationBufferPointer: UnsafeMutablePointer<UInt8>

    init(handle: MediumHandle, algorithm: compression_algorithm, encoding: String) throws {
        self.handle = handle
        self.headers = ["Content-Encoding": encoding]
            .merging(handle.headers, uniquingKeysWith: { (_, new) in new })

        let status = compression_stream_init(&stream, COMPRESSION_STREAM_ENCODE, algorithm)
        guard status != COMPRESSION_STATUS_ERROR else {
            throw AvroDataExtractionError.decodingError
        }

        destinationBufferPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        stream.src_size = 0
        stream.dst_ptr = destinationBufferPointer
        stream.dst_size = bufferSize
    }

    func append(data: Data) throws {
        try append(data: data, flags: 0)
    }

    func append(data: Data, flags: Int32) throws {
        var status = COMPRESSION_STATUS_END

        stream.src_size = data.count

        repeat {
            status = data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
                stream.src_ptr = bytes.baseAddress!.advanced(by: data.count - stream.src_size).assumingMemoryBound(to: UInt8.self)
                return compression_stream_process(&stream, flags)
            }

            switch status {
            case COMPRESSION_STATUS_OK,
                 COMPRESSION_STATUS_END:
                // Get the number of bytes put in the destination buffer. This is the difference between
                // stream.dst_size before the call (here bufferSize), and stream.dst_size after the call.
                try appendBuffer()
            case COMPRESSION_STATUS_ERROR:
                os_log("Failed to encode GZIP data")
                throw RequestMediumError.encodingError
            default:
                os_log("Failed to encode unknown GZIP status")
            }
        } while status == COMPRESSION_STATUS_OK && stream.src_size > 0
    }

    private func appendBuffer() throws {
        let count = bufferSize - stream.dst_size

        if count > 0 {
            let outputData = Data(bytesNoCopy: destinationBufferPointer,
                                  count: count,
                                  deallocator: .none)

            // Write all produced bytes to the output file.
            try handle.append(data: outputData)

            // Reset the stream to receive the next batch of output.
            stream.dst_size = bufferSize
        }

        stream.dst_ptr = destinationBufferPointer
    }


    func finalize() throws {
        defer {
            compression_stream_destroy(&stream)
            destinationBufferPointer.deallocate()
        }

        try append(data: Data(), flags: Int32(COMPRESSION_STREAM_FINALIZE.rawValue))

        try handle.finalize()
    }
}
