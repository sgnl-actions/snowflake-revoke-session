import script from '../src/script.mjs';
import { SGNL_USER_AGENT } from '@sgnl-actions/utils';

describe('Snowflake Revoke Session Script', () => {
  const bearerContext = {
    environment: {
      ADDRESS: 'https://api.snowflakecomputing.com'
    },
    secrets: {
      BEARER_AUTH_TOKEN: 'Bearer test-snowflake-token-123456'
    },
    outputs: {}
  };

  const oauthClientCredsContext = {
    environment: {
      ADDRESS: 'https://api.snowflakecomputing.com',
      OAUTH2_CLIENT_CREDENTIALS_CLIENT_ID: 'client-id',
      OAUTH2_CLIENT_CREDENTIALS_TOKEN_URL: 'https://example.snowflakecomputing.com/oauth/token'
    },
    secrets: {
      OAUTH2_CLIENT_CREDENTIALS_CLIENT_SECRET: 'client-secret'
    },
    outputs: {}
  };

  const oauthAuthCodeContext = {
    environment: {
      ADDRESS: 'https://api.snowflakecomputing.com'
    },
    secrets: {
      OAUTH2_AUTHORIZATION_CODE_ACCESS_TOKEN: 'oauth-access-token'
    },
    outputs: {}
  };

  const successFetch = async (_url, _options) => ({
    ok: true,
    status: 200,
    json: async () => ({ statementHandle: 'abc123' }),
    text: async () => ''
  });

  beforeEach(() => {
    global.console.log = () => {};
    global.console.error = () => {};
    global.console.warn = () => {};
  });

  describe('invoke handler', () => {
    test('should throw error for missing username', async () => {
      await expect(script.invoke({}, bearerContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should throw error for empty username', async () => {
      await expect(script.invoke({ username: '' }, bearerContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should throw error for whitespace-only username', async () => {
      await expect(script.invoke({ username: '   ' }, bearerContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should succeed and return result', async () => {
      global.fetch = successFetch;

      const result = await script.invoke({ username: 'testuser' }, bearerContext);

      expect(result.username).toBe('testuser');
      expect(result.sessionsRevoked).toBe(true);
      expect(result.userDisabled).toBe('abc123');
      expect(result.userReEnabled).toBe('abc123');
      expect(result.revokedAt).toBeDefined();
    });

    test('should fall back to true when statementHandle is absent', async () => {
      global.fetch = async () => ({
        ok: true,
        status: 200,
        json: async () => ({}),
        text: async () => ''
      });

      const result = await script.invoke({ username: 'testuser' }, bearerContext);

      expect(result.userDisabled).toBe(true);
      expect(result.userReEnabled).toBe(true);
    });

    test('should parse delay in seconds', async () => {
      global.fetch = successFetch;

      // Just verify it completes without error — parseDuration('1ms') keeps test fast
      const result = await script.invoke({ username: 'testuser', delay: '1ms' }, bearerContext);
      expect(result.sessionsRevoked).toBe(true);
    });

    test('should parse delay in minutes', async () => {
      global.fetch = successFetch;

      const result = await script.invoke({ username: 'testuser', delay: '0m' }, bearerContext);
      expect(result.sessionsRevoked).toBe(true);
    });

    test('should parse delay in hours', async () => {
      global.fetch = successFetch;

      const result = await script.invoke({ username: 'testuser', delay: '0h' }, bearerContext);
      expect(result.sessionsRevoked).toBe(true);
    });

    test('should default delay to 100ms for invalid format', async () => {
      global.fetch = successFetch;

      // Uses an invalid duration string — falls back to 100ms default
      const result = await script.invoke({ username: 'testuser', delay: 'invalid' }, bearerContext);
      expect(result.sessionsRevoked).toBe(true);
    });

    test('should set KEYPAIR_JWT token type for Bearer auth', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser' }, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('KEYPAIR_JWT');
    });

    test('should set OAUTH token type for OAuth2 Client Credentials auth', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        if (url.includes('/oauth/token')) {
          return { ok: true, status: 200, json: async () => ({ access_token: 'oauth-token-value' }), text: async () => '' };
        }
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser' }, oauthClientCredsContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('OAUTH');
    });

    test('should set OAUTH token type for OAuth2 Authorization Code auth', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser' }, oauthAuthCodeContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('OAUTH');
    });

    test('should omit token type header when no auth secrets are configured', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      const ctx = { environment: { ADDRESS: 'https://api.snowflakecomputing.com' }, secrets: {}, outputs: {} };
      await script.invoke({ username: 'testuser' }, ctx);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBeUndefined();
    });

    test('should override token type to PROGRAMMATIC_ACCESS_TOKEN', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser', snowflake_auth_token_type: 'PROGRAMMATIC_ACCESS_TOKEN' }, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('PROGRAMMATIC_ACCESS_TOKEN');
    });

    test('should override token type to WORKLOAD_IDENTITY_FEDERATION', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser', snowflake_auth_token_type: 'WORKLOAD_IDENTITY_FEDERATION' }, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('WORKLOAD_IDENTITY_FEDERATION');
    });

    test('should omit token type header when snowflake_auth_token_type is AUTO', async () => {
      let capturedHeaders;
      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser', snowflake_auth_token_type: 'AUTO' }, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBeUndefined();
    });

    test('should throw FatalError for invalid snowflake_auth_token_type value', async () => {
      await expect(script.invoke({ username: 'testuser', snowflake_auth_token_type: 'INVALID_VALUE' }, bearerContext))
        .rejects.toThrow('Invalid snowflake_auth_token_type "INVALID_VALUE"');
    });

    test('should include User-Agent header in API calls', async () => {
      let capturedOptions;
      global.fetch = async (url, options) => {
        capturedOptions = options;
        return { ok: true, status: 200, json: async () => ({ statementHandle: 'abc123' }), text: async () => '' };
      };

      await script.invoke({ username: 'testuser' }, bearerContext);

      expect(capturedOptions.headers['User-Agent']).toBe(SGNL_USER_AGENT);
    });

    test('should throw RetryableError on 429 rate limit', async () => {
      global.fetch = async () => ({
        ok: false,
        status: 429,
        statusText: 'Too Many Requests',
        text: async () => 'rate limited'
      });

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toBe('Snowflake API rate limit exceeded');
      expect(err.retryable).toBe(true);
    });

    test('should throw FatalError on 401 unauthorized', async () => {
      global.fetch = async () => ({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
        text: async () => 'unauthorized'
      });

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toBe('Invalid or expired authentication token');
      expect(err.retryable).toBe(false);
    });

    test('should throw FatalError on 403 forbidden', async () => {
      global.fetch = async () => ({
        ok: false,
        status: 403,
        statusText: 'Forbidden',
        text: async () => 'forbidden'
      });

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toBe('Insufficient permissions to execute statement');
      expect(err.retryable).toBe(false);
    });

    test('should throw FatalError on 422 unprocessable entity', async () => {
      global.fetch = async () => ({
        ok: false,
        status: 422,
        statusText: 'Unprocessable Entity',
        text: async () => 'bad sql'
      });

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toContain('Invalid SQL statement');
      expect(err.retryable).toBe(false);
    });

    test('should throw RetryableError on 500 server error', async () => {
      global.fetch = async () => ({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        text: async () => 'server error'
      });

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toContain('Snowflake API server error: 500');
      expect(err.retryable).toBe(true);
    });

    test('should throw FatalError on other non-ok status', async () => {
      global.fetch = async () => ({
        ok: false,
        status: 400,
        statusText: 'Bad Request',
        text: async () => 'bad request body'
      });

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toContain('Failed to execute statement: 400');
      expect(err.retryable).toBe(false);
    });

    test('should wrap unexpected errors as FatalError', async () => {
      global.fetch = async () => { throw new TypeError('network failure'); };

      const err = await script.invoke({ username: 'testuser' }, bearerContext).catch(e => e);
      expect(err.message).toContain('network failure');
      expect(err.retryable).toBe(false);
    });
  });

  describe('error handler', () => {
    test('should re-throw error for framework to handle', async () => {
      const params = { username: 'testuser', error: new Error('Network timeout') };

      await expect(script.error(params, bearerContext))
        .rejects.toThrow('Network timeout');
    });
  });

  describe('halt handler', () => {
    test('should handle graceful shutdown', async () => {
      const result = await script.halt({ username: 'testuser', reason: 'timeout' }, bearerContext);

      expect(result.username).toBe('testuser');
      expect(result.reason).toBe('timeout');
      expect(result.haltedAt).toBeDefined();
      expect(result.cleanupCompleted).toBe(true);
    });

    test('should handle halt with missing params', async () => {
      const result = await script.halt({ reason: 'system_shutdown' }, bearerContext);

      expect(result.username).toBe('unknown');
      expect(result.reason).toBe('system_shutdown');
      expect(result.cleanupCompleted).toBe(true);
    });
  });
});
