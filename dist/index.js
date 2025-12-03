// SGNL Job Script - Auto-generated bundle
'use strict';

/**
 * SGNL Actions - Authentication Utilities
 *
 * Shared authentication utilities for SGNL actions.
 * Supports: Bearer Token, Basic Auth, OAuth2 Client Credentials, OAuth2 Authorization Code
 */

/**
 * Get OAuth2 access token using client credentials flow
 * @param {Object} config - OAuth2 configuration
 * @param {string} config.tokenUrl - Token endpoint URL
 * @param {string} config.clientId - Client ID
 * @param {string} config.clientSecret - Client secret
 * @param {string} [config.scope] - OAuth2 scope
 * @param {string} [config.audience] - OAuth2 audience
 * @param {string} [config.authStyle] - Auth style: 'InParams' or 'InHeader' (default)
 * @returns {Promise<string>} Access token
 */
async function getClientCredentialsToken(config) {
  const { tokenUrl, clientId, clientSecret, scope, audience, authStyle } = config;

  if (!tokenUrl || !clientId || !clientSecret) {
    throw new Error('OAuth2 Client Credentials flow requires tokenUrl, clientId, and clientSecret');
  }

  const params = new URLSearchParams();
  params.append('grant_type', 'client_credentials');

  if (scope) {
    params.append('scope', scope);
  }

  if (audience) {
    params.append('audience', audience);
  }

  const headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json'
  };

  if (authStyle === 'InParams') {
    params.append('client_id', clientId);
    params.append('client_secret', clientSecret);
  } else {
    const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
    headers['Authorization'] = `Basic ${credentials}`;
  }

  const response = await fetch(tokenUrl, {
    method: 'POST',
    headers,
    body: params.toString()
  });

  if (!response.ok) {
    let errorText;
    try {
      const errorData = await response.json();
      errorText = JSON.stringify(errorData);
    } catch {
      errorText = await response.text();
    }
    throw new Error(
      `OAuth2 token request failed: ${response.status} ${response.statusText} - ${errorText}`
    );
  }

  const data = await response.json();

  if (!data.access_token) {
    throw new Error('No access_token in OAuth2 response');
  }

  return data.access_token;
}

/**
 * Get the Authorization header value from context using available auth method.
 * Supports: Bearer Token, Basic Auth, OAuth2 Authorization Code, OAuth2 Client Credentials
 *
 * @param {Object} context - Execution context with environment and secrets
 * @param {Object} context.environment - Environment variables
 * @param {Object} context.secrets - Secret values
 * @returns {Promise<string>} Authorization header value (e.g., "Bearer xxx" or "Basic xxx")
 */
async function getAuthorizationHeader(context) {
  const env = context.environment || {};
  const secrets = context.secrets || {};

  // Method 1: Simple Bearer Token
  if (secrets.BEARER_AUTH_TOKEN) {
    const token = secrets.BEARER_AUTH_TOKEN;
    return token.startsWith('Bearer ') ? token : `Bearer ${token}`;
  }

  // Method 2: Basic Auth (username + password)
  if (secrets.BASIC_PASSWORD && secrets.BASIC_USERNAME) {
    const credentials = Buffer.from(`${secrets.BASIC_USERNAME}:${secrets.BASIC_PASSWORD}`).toString('base64');
    return `Basic ${credentials}`;
  }

  // Method 3: OAuth2 Authorization Code - use pre-existing access token
  if (secrets.OAUTH2_AUTHORIZATION_CODE_ACCESS_TOKEN) {
    const token = secrets.OAUTH2_AUTHORIZATION_CODE_ACCESS_TOKEN;
    return token.startsWith('Bearer ') ? token : `Bearer ${token}`;
  }

  // Method 4: OAuth2 Client Credentials - fetch new token
  if (secrets.OAUTH2_CLIENT_CREDENTIALS_CLIENT_SECRET) {
    const tokenUrl = env.OAUTH2_CLIENT_CREDENTIALS_TOKEN_URL;
    const clientId = env.OAUTH2_CLIENT_CREDENTIALS_CLIENT_ID;
    const clientSecret = secrets.OAUTH2_CLIENT_CREDENTIALS_CLIENT_SECRET;

    if (!tokenUrl || !clientId) {
      throw new Error('OAuth2 Client Credentials flow requires TOKEN_URL and CLIENT_ID in env');
    }

    const token = await getClientCredentialsToken({
      tokenUrl,
      clientId,
      clientSecret,
      scope: env.OAUTH2_CLIENT_CREDENTIALS_SCOPE,
      audience: env.OAUTH2_CLIENT_CREDENTIALS_AUDIENCE,
      authStyle: env.OAUTH2_CLIENT_CREDENTIALS_AUTH_STYLE
    });

    return `Bearer ${token}`;
  }

  throw new Error(
    'No authentication configured. Provide one of: ' +
    'BEARER_AUTH_TOKEN, BASIC_USERNAME/BASIC_PASSWORD, ' +
    'OAUTH2_AUTHORIZATION_CODE_ACCESS_TOKEN, or OAUTH2_CLIENT_CREDENTIALS_*'
  );
}

/**
 * Get the base URL/address for API calls
 * @param {Object} params - Request parameters
 * @param {string} [params.address] - Address from params
 * @param {Object} context - Execution context
 * @returns {string} Base URL
 */
function getBaseUrl(params, context) {
  const env = context.environment || {};
  const address = params?.address || env.ADDRESS;

  if (!address) {
    throw new Error('No URL specified. Provide address parameter or ADDRESS environment variable');
  }

  // Remove trailing slash if present
  return address.endsWith('/') ? address.slice(0, -1) : address;
}

class RetryableError extends Error {
  constructor(message) {
    super(message);
    this.retryable = true;
  }
}

class FatalError extends Error {
  constructor(message) {
    super(message);
    this.retryable = false;
  }
}

function validateInputs(params) {
  if (!params.username || typeof params.username !== 'string' || params.username.trim() === '') {
    throw new FatalError('Invalid or missing username parameter');
  }
}

function parseDuration(durationStr) {
  if (!durationStr) return 100; // default 100ms

  const match = durationStr.match(/^(\d+(?:\.\d+)?)(ms|s|m|h)?$/i);
  if (!match) {
    console.warn(`Invalid duration format: ${durationStr}, using default 100ms`);
    return 100;
  }

  const value = parseFloat(match[1]);
  const unit = (match[2] || 'ms').toLowerCase();

  switch (unit) {
    case 'ms':
      return value;
    case 's':
      return value * 1000;
    case 'm':
      return value * 60 * 1000;
    case 'h':
      return value * 60 * 60 * 1000;
    default:
      return value;
  }
}

async function executeStatement(statement, authHeader, baseUrl) {
  const url = `${baseUrl.replace(/\/$/, '')}/api/v2/statements`;

  // Extract the token from the Authorization header to determine type
  const token = authHeader.replace(/^Bearer\s+/i, '');
  const tokenType = determineTokenType(token);

  const headers = {
    'Authorization': authHeader,
    'Content-Type': 'application/json',
    'Accept': '*/*'
  };

  // Add token type header if specified
  if (tokenType) {
    headers['X-Snowflake-Authorization-Token-Type'] = tokenType;
  }

  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({ statement })
  });

  if (!response.ok) {
    const responseText = await response.text();

    if (response.status === 429) {
      throw new RetryableError('Snowflake API rate limit exceeded');
    }

    if (response.status === 401) {
      throw new FatalError('Invalid or expired authentication token');
    }

    if (response.status === 403) {
      throw new FatalError('Insufficient permissions to execute statement');
    }

    if (response.status === 422) {
      // Unprocessable Entity - likely invalid SQL statement
      throw new FatalError(`Invalid SQL statement: ${responseText}`);
    }

    if (response.status >= 500) {
      throw new RetryableError(`Snowflake API server error: ${response.status}`);
    }

    throw new FatalError(`Failed to execute statement: ${response.status} ${response.statusText} - ${responseText}`);
  }

  const data = await response.json();
  return data;
}

function determineTokenType(authToken) {
  // If token appears to be JWT (has three dots), assume KEYPAIR_JWT
  // Otherwise, assume OAUTH
  if (authToken && authToken.split('.').length === 3) {
    return 'KEYPAIR_JWT';
  }
  return 'OAUTH';
}

var script = {
  /**
   * Main execution handler - revokes all active sessions for a Snowflake user
   * @param {Object} params - Job input parameters
   * @param {string} params.username - The Snowflake username to revoke sessions for (required)
   * @param {string} params.delay - Optional delay between disable and re-enable operations (e.g., 100ms, 1s)
   * @param {string} params.address - Optional Snowflake API base URL
   *
   * @param {Object} context - Execution context with secrets and environment
   * @param {string} context.environment.ADDRESS - Snowflake API base URL
   *
   * The configured auth type will determine which of the following environment variables and secrets are available
   * @param {string} context.secrets.BEARER_AUTH_TOKEN
   *
   * @param {string} context.secrets.BASIC_USERNAME
   * @param {string} context.secrets.BASIC_PASSWORD
   *
   * @param {string} context.secrets.OAUTH2_CLIENT_CREDENTIALS_CLIENT_SECRET
   * @param {string} context.environment.OAUTH2_CLIENT_CREDENTIALS_AUDIENCE
   * @param {string} context.environment.OAUTH2_CLIENT_CREDENTIALS_AUTH_STYLE
   * @param {string} context.environment.OAUTH2_CLIENT_CREDENTIALS_CLIENT_ID
   * @param {string} context.environment.OAUTH2_CLIENT_CREDENTIALS_SCOPE
   * @param {string} context.environment.OAUTH2_CLIENT_CREDENTIALS_TOKEN_URL
   *
   * @param {string} context.secrets.OAUTH2_AUTHORIZATION_CODE_ACCESS_TOKEN
   *
   * @returns {Promise<Object>} Action result
   */
  invoke: async (params, context) => {
    console.log('Starting Snowflake Revoke Session action');

    try {
      validateInputs(params);

      const { username, delay } = params;

      console.log(`Processing username: ${username}`);

      // Get authorization header
      const authHeader = await getAuthorizationHeader(context);

      // Get base URL
      const baseUrl = getBaseUrl(params, context);

      // Parse delay duration
      const delayMs = parseDuration(delay);

      // Step 1: Disable the user (this revokes all sessions)
      console.log(`Disabling user: ${username}`);
      const disableStatement = `ALTER USER ${username} SET DISABLED = TRUE`;
      const disableResult = await executeStatement(disableStatement, authHeader, baseUrl);

      // Add delay between operations
      console.log(`Waiting ${delayMs}ms before re-enabling user`);
      await new Promise(resolve => setTimeout(resolve, delayMs));

      // Step 2: Re-enable the user
      console.log(`Re-enabling user: ${username}`);
      const enableStatement = `ALTER USER ${username} SET DISABLED = FALSE`;
      const enableResult = await executeStatement(enableStatement, authHeader, baseUrl);

      const result = {
        username,
        sessionsRevoked: true,
        userDisabled: disableResult.statementHandle || true,
        userReEnabled: enableResult.statementHandle || true,
        revokedAt: new Date().toISOString()
      };

      console.log(`Successfully revoked sessions for user: ${username}`);
      return result;

    } catch (error) {
      console.error(`Error revoking Snowflake sessions: ${error.message}`);

      if (error instanceof RetryableError || error instanceof FatalError) {
        throw error;
      }

      throw new FatalError(`Unexpected error: ${error.message}`);
    }
  },

  /**
   * Error recovery handler - handles errors during session revocation
   *
   * @param {Object} params - Original params plus error information
   * @param {Object} context - Execution context
   *
   * @returns {Object} Recovery results
   */
  error: async (params, _context) => {
    const { error } = params;
    console.error(`Error handler invoked: ${error?.message}`);

    // Re-throw to let framework handle retries
    throw error;
  },

  /**
   * Halt handler - handles graceful shutdown
   *
   * @param {Object} params - Halt parameters including reason
   * @param {Object} context - Execution context
   *
   * @returns {Object} Halt results
   */
  halt: async (params, _context) => {
    const { reason, username } = params;
    console.log(`Job is being halted (${reason})`);

    return {
      username: username || 'unknown',
      reason: reason || 'unknown',
      haltedAt: new Date().toISOString(),
      cleanupCompleted: true
    };
  }
};

module.exports = script;
