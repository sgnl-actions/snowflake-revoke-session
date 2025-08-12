// SGNL Job Script - Auto-generated bundle
'use strict';

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

async function executeStatement(statement, token, tokenType) {
  const url = 'https://api.snowflakecomputing.com/api/v2/statements';
  
  const headers = {
    'Authorization': token.startsWith('Bearer ') ? token : `Bearer ${token}`,
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

function determineTokenType(token) {
  // If token appears to be JWT (has three dots), assume KEYPAIR_JWT
  // Otherwise, assume OAUTH
  if (token && token.split('.').length === 3) {
    return 'KEYPAIR_JWT';
  }
  return 'OAUTH';
}

var script = {
  invoke: async (params, context) => {
    console.log('Starting Snowflake Revoke Session action');
    
    try {
      validateInputs(params);
      
      const { username } = params;
      
      console.log(`Processing username: ${username}`);
      
      if (!context.secrets?.SNOWFLAKE_TOKEN) {
        throw new FatalError('Missing required secret: SNOWFLAKE_TOKEN');
      }
      
      const token = context.secrets.SNOWFLAKE_TOKEN;
      const tokenType = determineTokenType(token);
      
      // Step 1: Disable the user (this revokes all sessions)
      console.log(`Disabling user: ${username}`);
      const disableStatement = `ALTER USER ${username} SET DISABLED = TRUE`;
      const disableResult = await executeStatement(disableStatement, token, tokenType);
      
      // Add small delay between operations
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Step 2: Re-enable the user
      console.log(`Re-enabling user: ${username}`);
      const enableStatement = `ALTER USER ${username} SET DISABLED = FALSE`;
      const enableResult = await executeStatement(enableStatement, token, tokenType);
      
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

  error: async (params, _context) => {
    const { error } = params;
    console.error(`Error handler invoked: ${error?.message}`);
    
    // Re-throw to let framework handle retries
    throw error;
  },

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
