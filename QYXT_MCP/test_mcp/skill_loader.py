from pathlib import Path
from dataclasses import dataclass
import yaml
import re


@dataclass
class Skill:
    name: str
    description: str
    tools: list
    content: str


class SkillManager:

    def __init__(self, skill_dir="skills"):
        self.skills = []
        self.skill_map = {}
        skill_dir = Path(skill_dir)

        if not skill_dir.exists():
            raise FileNotFoundError(f"Skill目录不存在：{skill_dir}")

        for file in sorted(skill_dir.glob("*.md")):
            if file.name.startswith("."):
                continue

            print(f"Loading {file.name} ...")
            skill = self.load(file)

            if skill.name in self.skill_map:
                raise ValueError(f"重复的Skill名称：{skill.name}")

            self.skills.append(skill)
            self.skill_map[skill.name] = skill

        print(f"\n共加载 {len(self.skills)} 个Skill\n")

    def load(self, path: Path):
        text = path.read_text(encoding="utf-8")
        m = re.match(
            r"^---\s*\n(.*?)\n---\s*\n?(.*)$",
            text,
            flags=re.S,
        )

        if not m:
            raise RuntimeError(f"{path} 缺少Front Matter(---)")

        yaml_text = m.group(1)
        content = m.group(2).strip()

        try:
            metadata = yaml.safe_load(yaml_text) or {}
        except yaml.YAMLError as e:
            raise RuntimeError(f"{path} YAML解析失败：\n{e}")

        name = metadata.get("name")
        if not name:
            raise RuntimeError(f"{path} 缺少name字段")

        description = str(metadata.get("description", ""))
        tools = metadata.get("tools", [])

        if tools is None:
            tools = []

        if not isinstance(tools, list):
            raise RuntimeError(f"{path} tools必须是列表")

        return Skill(
            name=name,
            description=description,
            tools=tools,
            content=content,
        )

    def find(self, name):
        return self.skill_map.get(name)

    def select_skill(self, query: str):
        query = query.lower()
        if "轨迹" in query:
            return self.find("trajectory-analysis")
        if "异常" in query or "风险" in query:
            return self.find("abnormal-monitor")
        if "事故" in query or "应急" in query:
            return self.find("emergency-response")
        return self.find("person-vehicle-query")


if __name__ == '__main__':
    skill_manager = SkillManager()
    print(skill_manager)