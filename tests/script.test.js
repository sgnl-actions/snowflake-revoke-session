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

  beforeEach(() => {
    global.console.log = () => {};
    global.console.error = () => {};
    global.console.warn = () => {};
  });

  describe('invoke handler', () => {
    test('should throw error for missing username', async () => {
      const params = {};

      await expect(script.invoke(params, bearerContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should throw error for invalid username', async () => {
      const params = { username: '' };

      await expect(script.invoke(params, bearerContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should throw error for whitespace-only username', async () => {
      const params = { username: '   ' };

      await expect(script.invoke(params, bearerContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should reject Basic auth with FatalError', async () => {
      const params = { username: 'testuser' };
      const ctx = {
        environment: { ADDRESS: 'https://api.snowflakecomputing.com' },
        secrets: { BASIC_USERNAME: 'user', BASIC_PASSWORD: 'pass' },
        outputs: {}
      };

      await expect(script.invoke(params, ctx))
        .rejects.toThrow('Basic authentication is not supported by the Snowflake SQL API');
    });

    test('should set KEYPAIR_JWT token type for Bearer auth', async () => {
      const params = { username: 'testuser' };
      let capturedHeaders;

      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('KEYPAIR_JWT');
    });

    test('should set OAUTH token type for OAuth2 Client Credentials auth', async () => {
      const params = { username: 'testuser' };
      let capturedHeaders;

      global.fetch = async (url, options) => {
        // First call is the token exchange, second/third are the SQL API calls
        if (url.includes('/oauth/token')) {
          return {
            ok: true,
            status: 200,
            json: async () => ({ access_token: 'oauth-token-value' }),
            text: async () => ''
          };
        }
        capturedHeaders = options.headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, oauthClientCredsContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('OAUTH');
    });

    test('should set OAUTH token type for OAuth2 Authorization Code auth', async () => {
      const params = { username: 'testuser' };
      let capturedHeaders;

      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, oauthAuthCodeContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('OAUTH');
    });

    test('should override token type when SNOWFLAKE_AUTH_TOKEN_TYPE is set', async () => {
      const params = { username: 'testuser', snowflake_auth_token_type: 'PROGRAMMATIC_ACCESS_TOKEN' };
      let capturedHeaders;

      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('PROGRAMMATIC_ACCESS_TOKEN');
    });

    test('should set WORKLOAD_IDENTITY_FEDERATION token type when overridden', async () => {
      const params = { username: 'testuser', snowflake_auth_token_type: 'WORKLOAD_IDENTITY_FEDERATION' };
      let capturedHeaders;

      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBe('WORKLOAD_IDENTITY_FEDERATION');
    });

    test('should omit token type header when SNOWFLAKE_AUTH_TOKEN_TYPE is AUTO', async () => {
      const params = { username: 'testuser', snowflake_auth_token_type: 'AUTO' };
      let capturedHeaders;

      global.fetch = async (url, options) => {
        capturedHeaders = options.headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, bearerContext);

      expect(capturedHeaders['X-Snowflake-Authorization-Token-Type']).toBeUndefined();
    });

    test('should throw FatalError for invalid SNOWFLAKE_AUTH_TOKEN_TYPE value', async () => {
      const params = { username: 'testuser', snowflake_auth_token_type: 'INVALID_VALUE' };

      await expect(script.invoke(params, bearerContext))
        .rejects.toThrow('Invalid snowflake_auth_token_type "INVALID_VALUE"');
    });

    test('should include User-Agent header in API calls', async () => {
      const params = { username: 'testuser' };
      let capturedOptions;

      global.fetch = async (url, options) => {
        capturedOptions = options;
        return {
          ok: true,
          status: 200,
          json: async () => ({ statementHandle: 'abc123' }),
          text: async () => ''
        };
      };

      await script.invoke(params, bearerContext);

      expect(capturedOptions.headers['User-Agent']).toBe(SGNL_USER_AGENT);
    });
  });

  describe('error handler', () => {
    test('should re-throw error for framework to handle', async () => {
      const params = {
        username: 'testuser',
        error: new Error('Network timeout')
      };

      await expect(script.error(params, bearerContext))
        .rejects.toThrow('Network timeout');
    });
  });

  describe('halt handler', () => {
    test('should handle graceful shutdown', async () => {
      const params = { username: 'testuser', reason: 'timeout' };

      const result = await script.halt(params, bearerContext);

      expect(result.username).toBe('testuser');
      expect(result.reason).toBe('timeout');
      expect(result.haltedAt).toBeDefined();
      expect(result.cleanupCompleted).toBe(true);
    });

    test('should handle halt with missing params', async () => {
      const params = { reason: 'system_shutdown' };

      const result = await script.halt(params, bearerContext);

      expect(result.username).toBe('unknown');
      expect(result.reason).toBe('system_shutdown');
      expect(result.cleanupCompleted).toBe(true);
    });
  });
});
