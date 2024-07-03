from vanna.base import VannaBase
from groq import Groq


class GroqLLM(VannaBase):
    def __init__(self, config=None):
        VannaBase.__init__(self, config=config)

        if config is None:
            raise ValueError(
                "For Groq, config must be provided with an api_key and model"
            )

        if "api_key" not in config:
            raise ValueError("config must contain a Groq api_key")

        if "model" not in config:
            raise ValueError("config must contain a Groq model")

        self.client = Groq(api_key=config["api_key"])
        self.model = config["model"]

    def system_message(self, message: str) -> any:
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> any:
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> any:
        return {"role": "assistant", "content": message}

    def submit_prompt(self, prompt, **kwargs) -> str:
        chat_response = self.client.chat.completions.create(
            model=self.model,
            messages=prompt,
        )

        return chat_response.choices[0].message.content
