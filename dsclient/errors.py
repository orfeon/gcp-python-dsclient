class GCPError(Exception):

    def __init__(self, message, code=None):

        self.message = message
        self.code = code


class BigQueryError(GCPError):

    def __init__(self, reasons, messages):

        self.reasons = reasons
        self.messages = messages

        codes = []
        for reason in reasons:
            if reason in ["billingTierLimitExceeded","invalid","invalidQuery","resourceInUse","resourcesExceeded"]:
                code = 400
            elif reason in ["accessDenied","billingNotEnabled","blocked","rateLimitExceeded","responseTooLarge"]:
                code = 403
            elif reason == "notFound":
                code = 404
            elif reason == "duplicate":
                code = 409
            elif reason == "internalError":
                code = 500
            elif reason == "notImplemented":
                code = 501
            elif reason == "backendError":
                code = 503
            else:
                code = 0
            codes.append(code)

        self.codes = codes

    def __repr__(self):

        #message = ":".join([error["reason"] + error["message"] for error in job["status"]["errorResult"]])
        messages = [r + ": " + m for r, m in zip(self.reasons, self.messages)]
        return ", ".join(messages)

    __str__ = __repr__
