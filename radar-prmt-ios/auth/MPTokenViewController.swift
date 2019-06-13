//
//  MPTokenViewController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 13/05/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import UIKit
import RxSwift
import os.log

class MPTokenViewController : UIViewController {
    let disposeBag = DisposeBag()

    override func viewDidLoad() {
        super.viewDidLoad()

        if let baseUrl = UserDefaults.standard.url(forKey: "baseUrl") {
            var urlString = baseUrl.absoluteString
            if urlString.starts(with: "https://") {
                urlString.removeFirst("https://".count)
            }
            baseUrlField.text = urlString
        }
    }

    @IBOutlet weak var baseUrlField: UITextField!
    @IBOutlet weak var tokenField: UITextField!
    @IBAction func didEnterToken(_ sender: Any) {
        guard let url = validateUrl(),
                let authorizer = appDelegate.authController.authorizer else {
            return
        }
        authorizer.metaTokenLogin(url: url)
            .subscribeOn(ConcurrentDispatchQueueScheduler.init(qos: .background))
            .observeOn(MainScheduler.instance)
            .subscribe(weak: self, onNext: { weakSelf in { mpauth in
                    os_log("Retrieved MetaToken")
                    weakSelf.appDelegate.authController.auth.onNext(mpauth)
                    weakSelf.performSegue(withIdentifier: "mainFromToken", sender: self)
                }
            }, onError: { _ in { error in
                    os_log("Failed to retrieve MetaToken: %@", error.localizedDescription)
                }
            })
            .disposed(by: disposeBag)
    }

    private func validateUrl() -> URL? {
        var validationErrors = MPTokenValidationError()
        var baseUrl: String? = nil
        if var literalBaseUrl = baseUrlField.text, !literalBaseUrl.isEmpty {
            while literalBaseUrl.last == "/" {
                literalBaseUrl.removeLast()
            }
            if literalBaseUrl.hasSuffix("/managementportal") {
                literalBaseUrl.removeLast("/managementportal".count)
                while literalBaseUrl.last == "/" {
                    literalBaseUrl.removeLast()
                }
            }
            baseUrl = literalBaseUrl
        } else {
            validationErrors.insert(.baseUrlMissing)
        }

        var token: String? = nil
        if let literalToken = tokenField.text, !literalToken.isEmpty {
            if literalToken.rangeOfCharacter(from: CharacterSet.alphanumerics.inverted) != nil {
                validationErrors.insert(.tokenInvalid)
            } else {
                token = literalToken
            }
        } else {
            validationErrors.insert(.tokenMissing)
        }

        guard let validBaseUrl = baseUrl, let validToken = token else {
            // TODO: UI changes based on URL validation
            return nil
        }

        guard let url = URL(string: "https://\(validBaseUrl)/managementportal/api/meta-token/\(validToken)") else {
            validationErrors.insert(.baseUrlInvalid)
            // TODO: UI changes based on URL validation
            return nil
        }

        return url
    }


}

struct MPTokenValidationError: OptionSet {
    let rawValue: Int

    static let baseUrlMissing = MPTokenValidationError(rawValue: 1)
    static let tokenMissing = MPTokenValidationError(rawValue: 2)
    static let baseUrlInvalid = MPTokenValidationError(rawValue: 4)
    static let tokenInvalid = MPTokenValidationError(rawValue: 8)
}
