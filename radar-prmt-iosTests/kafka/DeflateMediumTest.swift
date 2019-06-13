//
//  GzipMediumTest.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 25/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import XCTest
import Compression
@testable import radar_prmt_ios

class DeflateMediumTest: XCTestCase {
    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() throws {
        let medium = DataRequestMedium()
        let deflateMedium = DeflateRequestMedium(medium: medium, algorithm: COMPRESSION_LZFSE, encoding: "lzfse")

        let handle = try deflateMedium.start(upload: RecordSetUpload())
        let dataHandle = ((handle as! DeflateMediumHandle).handle as! DataMediumHandle)
        XCTAssertEqual(Data([]), dataHandle.data)

        try handle.append(data: Data(base64Encoded: "aGVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlZWVlbGxv")!) // = heeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeello
        try handle.finalize()

        print(dataHandle.data.base64EncodedString())

        XCTAssertEqual("YnZ4bioAAAASAAAAmAFoZfAP42xsbwYAAAAAAAAAYnZ4JA==", dataHandle.data.base64EncodedString())
    }
}
