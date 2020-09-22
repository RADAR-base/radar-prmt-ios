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

        baseUrlField.delegate = self
        tokenField.delegate = self
    }

    @IBOutlet weak var statusLabel: UILabel!
    @IBOutlet weak var baseUrlField: UITextField!
    @IBOutlet weak var tokenField: UITextField!
    @IBAction func didEnterToken(_ sender: Any) {
        statusLabel.isHidden = true
        guard let url = validateUrl() else {
            os_log("Not proceeding, invalid data")
            return
        }
        appDelegate.authController.login(to: url)
            .subscribeOn(ConcurrentDispatchQueueScheduler.init(qos: .background))
            .observeOn(MainScheduler.instance)
            .subscribe(weak: self, onNext: { weakSelf in { mpauth in
                    os_log("Retrieved MetaToken")
                    weakSelf.performSegue(withIdentifier: "mainFromToken", sender: self)
                }
            }, onError: { weakSelf in { error in
                os_log("Failed to retrieve MetaToken: %@", error.localizedDescription)
                if let mpError = error as? MPAuthError {
                    switch (mpError) {
                    case .unauthorized:
                        weakSelf.showError(message: "This app is not correctly configured in the RADAR-base installation.")
                    case .tokenAlreadyUsed:
                        weakSelf.showError(message: "This token has been used. Please generate a new one.")
                    default:
                        weakSelf.showError(message: "Cannot log in: \(mpError.errorDescription!)")
                    }
                } else {
                    weakSelf.showError(message: "Cannot log in.")
                }
            }})
            .disposed(by: disposeBag)
    }

    private func validateUrl() -> URL? {
        guard let token = tokenField.text, !token.isEmpty else {
            showError(message: "Token is missing", fields: [tokenField])
            return nil
        }

        guard token.rangeOfCharacter(from: CharacterSet.alphanumerics.inverted) == nil else {
            showError(message: "Token text is not valid", fields: [tokenField])
            return nil
        }

        guard var baseUrl = baseUrlField.text, !baseUrl.isEmpty else {
            showError(message: "Base URL is missing", fields: [baseUrlField])
            return nil
        }

        while baseUrl.last == "/" {
            baseUrl.removeLast()
        }
        if baseUrl.hasSuffix("/managementportal") {
            baseUrl.removeLast("/managementportal".count)
            while baseUrl.last == "/" {
                baseUrl.removeLast()
            }
        }

        guard let url = URL(string: "https://\(baseUrl)/managementportal/api/meta-token/\(token)") else {
            showError(message: "Base URL is not a valid URL", fields: [tokenField])
            return nil
        }

        return url
    }

    private func showError(message: String, fields: [UITextField] = []) {
        fields.forEach {
            $0.backgroundColor = UIColor(hue: 0.0, saturation: 0.1, brightness: 1.0, alpha: 1.0)
        }
        os_log("Error registering token: %@", message)
        statusLabel.text = message
        statusLabel.isHidden = false
    }

    @IBAction
    private func resetValid(_ sender: Any) {
        if let field = sender as? UITextField {
            field.backgroundColor = UIColor.white
        }
    }
}

extension MPTokenViewController: UITextFieldDelegate {
    func textFieldShouldReturn(_ textField: UITextField) -> Bool {
        if textField == baseUrlField {
            self.view.endEditing(true)
        } else {
            baseUrlField.becomeFirstResponder()
        }
        return true
    }

}
