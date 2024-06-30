from vanna.base import VannaBase
import boto3


class AWSBedrockLLM(VannaBase):
    def __init__(self, config=None):
        VannaBase.__init__(self, config=config)

        if config is None:
            raise ValueError(
                "For AWS Bedrock, config must be provided with an api_key and model"
            )

        if "api_key" not in config:
            raise ValueError("config must contain an AWS Bedrock api_key")

        if "model" not in config:
            raise ValueError("config must contain an AWS Bedrock model")

        self.client = boto3.client(
            'bedrock-runtime',
            aws_access_key_id=config["api_key"],
            aws_secret_access_key=config["api_secret"],
            region_name=config.get("region", "us-east-1")
        )
        self.model = config["model"]

    def system_message(self, message: str) -> any:
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> any:
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> any:
        return {"role": "assistant", "content": message}

    def submit_prompt(self, prompt, **kwargs) -> str:
        if prompt is None:
            raise Exception("Prompt is None")

        if len(prompt) == 0:
            raise Exception("Prompt is empty")

        # Count the number of tokens in the message log
        # Use 4 as an approximation for the number of characters per token
        num_tokens = 0
        for message in prompt:
            num_tokens += len(message["content"]) / 4

        response = self.client.invoke_model(
            modelId=self.model,
            messages=prompt,
            max_tokens=kwargs.get("max_tokens", 500),
            temperature=kwargs.get("temperature", 0.7)
        )

        print(f"Response: {response}")
        # Find the first response from the chatbot that has text in it (some responses may not have text)
        for choice in response['choices']:
            if "text" in choice:
                return choice["text"]

        # If no response with text is found, return the first response's content (which may be empty)
        return response['choices'][0]['message']['content']