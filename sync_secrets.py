import boto3

def get_secret(secret_name, region_name='us-east-1') -> dict:
    """
    Get secret from AWS Secrets Manager
    :param secret_name: secret name in AWS Secrets Manager
    :param region_name: region name
    :return: dictionary that contains the secret
    """
    session = boto3.session.Session(profile_name='udacity')
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = get_secret_value_response['SecretBinary']
    return secret


if __name__ == '__main__':
    sec = get_secret('udacity_dend_secret')
    print(sec)