import json
import re
import traceback
from typing import Any, Callable, Dict, List, Union


class MinimalChainable:
    """
    Sequential prompt chaining with context and output back-references.
    """

    @staticmethod
    def run(
        context: Dict[str, Any], model: Any, callable: Callable, prompts: List[str]
    ) -> List[Any]:
        # Initialize an empty list to store the outputs
        output = []
        context_filled_prompts = []

        # Iterate over each prompt with its index
        for i, prompt in enumerate(prompts):
            # Iterate over each key-value pair in the context
            for key, value in context.items():
                # Replace the key with its value
                prompt = prompt.replace("{{" + key + "}}", str(value))

            # Replace references to previous outputs
            for j in range(i, 0, -1):
                previous_output = output[i - j]
                if isinstance(previous_output, dict):
                    if f"{{{{output[-{j}]}}}}" in prompt:
                        print(
                            f"Detected dict reference in prompt, replacing with {json.dumps(previous_output)}"
                        )
                        prompt = prompt.replace(
                            f"{{{{output[-{j}]}}}}", json.dumps(previous_output)
                        )
                    for key, value in previous_output.items():
                        prompt = prompt.replace(
                            f"{{{{output[-{j}].{key}}}}}", str(value)
                        )
                else:
                    prompt = prompt.replace(
                        f"{{{{output[-{j}]}}}}", str(previous_output)
                    )
            print(f"Prompt after output replacement {i}: {prompt}")

            # json_prompt = model.system_message('Ensure your output is in JSON format with the correct keys and double qoutes around each key and value. For exmple: {"key": "value"}. NEVER forget to use the double qoutes as this will cause major errors! Use the escape character for any special characters like double qoutes and backslash. The json returned should be able to be parsed by the json.loads function.')
            structured_prompt = [model.user_message(prompt)]

            context_filled_prompts.append(structured_prompt)

            # Call the provided callable with the processed prompt

            result = callable(structured_prompt)

            # Try to parse the result as JSON, handling markdown-wrapped JSON
            try:
                # json_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", result)
                json_match = re.search(r"{.*}", result)
                if json_match:
                    result = json.loads(result.replace("'", '"'))
            except json.JSONDecodeError:
                print(f"Error decoding JSON")
                traceback.print_exc()

            # Append the result to the output list
            output.append(result)

        return output, context_filled_prompts

    @staticmethod
    def to_delim_text_file(name: str, content: List[Union[str, dict]]) -> str:
        result_string = ""
        with open(f"{name}.txt", "w") as outfile:
            for i, item in enumerate(content, 1):
                if isinstance(item, dict):
                    item = json.dumps(item)
                if isinstance(item, list):
                    item = json.dumps(item)
                chain_text_delim = (
                    f"{'🔗' * i} -------- Prompt Chain Result #{i} -------------\n\n"
                )
                outfile.write(chain_text_delim)
                outfile.write(item)
                outfile.write("\n\n")

                result_string += chain_text_delim + item + "\n\n"

        return result_string
