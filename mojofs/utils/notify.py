def extract_req_params(req):
    """Extract request parameters from S3Request, mainly header information."""
    params = {}
    for key, value in req.headers.items():
        params[str(key)] = str(value)
    return params

def extract_req_params_header(head):
    """Extract request parameters from hyper::HeaderMap, mainly header information.
    This function is useful when you have a raw HTTP request and need to extract parameters.
    """
    params = {}
    for key, value in head.items():
        params[str(key)] = str(value)
    return params

def extract_resp_elements(resp):
    """Extract response elements from S3Response, mainly header information."""
    params = {}
    for key, value in resp.headers.items():
        params[str(key)] = str(value)
    return params

def get_request_host(headers):
    """Get host from header information."""
    return headers.get("host", "")

def get_request_user_agent(headers):
    """Get user-agent from header information."""
    return headers.get("user-agent", "")