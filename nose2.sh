# Make sure tests will use mocked environment, in case credentials are available
# via another mechanism
export AWS_ACCESS_KEY_ID="testing"
export AWS_SECRET_ACCESS_KEY="testing"
export AWS_SECURITY_TOKEN="testing"
export AWS_SESSION_TOKEN="testing"

exec nose2 $*
