import script from '../src/script.mjs';
import { SGNL_USER_AGENT } from '@sgnl-actions/utils';

describe('Snowflake Revoke Session Script', () => {
  const mockContext = {
    environment: {
      ADDRESS: 'https://api.snowflakecomputing.com'
    },
    secrets: {
      BEARER_AUTH_TOKEN: 'Bearer test-snowflake-token-123456'
    },
    outputs: {}
  };

  beforeEach(() => {
    // Mock console to avoid noise in tests
    global.console.log = () => {};
    global.console.error = () => {};
  });

  describe('invoke handler', () => {
    test('should throw error for missing username', async () => {
      const params = {};

      await expect(script.invoke(params, mockContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should throw error for invalid username', async () => {
      const params = {
        username: ''
      };

      await expect(script.invoke(params, mockContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should validate empty username', async () => {
      const params = {
        username: '   '
      };

      await expect(script.invoke(params, mockContext))
        .rejects.toThrow('Invalid or missing username parameter');
    });

    test('should include User-Agent header in API calls', async () => {
      const params = {
        username: 'testuser'
      };

      let capturedOptions;
      global.fetch = async (url, options) => {
        capturedOptions = options;
        return {
          ok: true,
          status: 200,
          json: async () => ({ message: 'Session revoked' }),
          text: async () => 'Session revoked'
        };
      };

      await script.invoke(params, mockContext);

      expect(capturedOptions.headers['User-Agent']).toBe(SGNL_USER_AGENT);
    });

    // Note: Testing actual Snowflake API calls would require mocking fetch
    // or integration tests with real Snowflake credentials
  });

  describe('error handler', () => {
    test('should re-throw error for framework to handle', async () => {
      const params = {
        username: 'testuser',
        error: new Error('Network timeout')
      };

      await expect(script.error(params, mockContext))
        .rejects.toThrow('Network timeout');
    });
  });

  describe('halt handler', () => {
    test('should handle graceful shutdown', async () => {
      const params = {
        username: 'testuser',
        reason: 'timeout'
      };

      const result = await script.halt(params, mockContext);

      expect(result.username).toBe('testuser');
      expect(result.reason).toBe('timeout');
      expect(result.haltedAt).toBeDefined();
      expect(result.cleanupCompleted).toBe(true);
    });

    test('should handle halt with missing params', async () => {
      const params = {
        reason: 'system_shutdown'
      };

      const result = await script.halt(params, mockContext);

      expect(result.username).toBe('unknown');
      expect(result.reason).toBe('system_shutdown');
      expect(result.cleanupCompleted).toBe(true);
    });
  });
});