const temporaryMembers = {
  'demo.resident': {
    pin: '123456',
    display_name: 'Demo Resident'
  },
  'ascendant.path': {
    pin: '654321',
    display_name: 'Ascendant Path'
  }
};

function cleanInput(value = '') {
  return String(value).trim();
}

function buildRedirect(location) {
  return {
    statusCode: 302,
    headers: {
      Location: location,
      'Cache-Control': 'no-store'
    }
  };
}

function parseFormBody(event) {
  const rawBody = event.isBase64Encoded
    ? Buffer.from(event.body || '', 'base64').toString('utf8')
    : event.body || '';

  return new URLSearchParams(rawBody);
}

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return buildRedirect('/login.html?error=invalid_request');
  }

  try {
    const formData = parseFormBody(event);
    const slUsername = cleanInput(formData.get('sl_username'));
    const loginPin = cleanInput(formData.get('login_pin'));

    if (!slUsername || !loginPin) {
      return buildRedirect(
        `/login.html?error=missing_fields&user=${encodeURIComponent(slUsername)}`
      );
    }

    const usernameKey = slUsername.toLowerCase();
    const member = temporaryMembers[usernameKey];

    if (!member) {
      return buildRedirect(
        `/login.html?error=no_account&user=${encodeURIComponent(slUsername)}`
      );
    }

    if (member.pin !== loginPin) {
      return buildRedirect(
        `/login.html?error=invalid_pin&user=${encodeURIComponent(slUsername)}`
      );
    }

    return buildRedirect(
      `/dashboard.html?display_name=${encodeURIComponent(member.display_name)}&sl_username=${encodeURIComponent(slUsername)}`
    );
  } catch (error) {
    console.error('Authentication function failed:', error);
    return buildRedirect('/login.html?error=server_error');
  }
};
