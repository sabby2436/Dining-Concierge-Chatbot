import json
import boto3

client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    lastUserMessage = event.get('messages')
    botMessage = "Something went wrong!! Please try again"
    
    if lastUserMessage is None or len(lastUserMessage) < 1:
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }
    
    lastUserMessage = lastUserMessage[0]['unstructured']['text']
    # Update the user id, so it is different for different user
    response = client.post_text(botName='DiningBot',
        botAlias='chatbotCncierge',
        userId='testuser',
        inputText=lastUserMessage)
    
    if response['message'] is not None or len(response['message']) > 0:
        botMessage = response['message']
    
    print("Bot message", botMessage)
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
    return {
        'statusCode': 200,
        'messages': botResponse
    }