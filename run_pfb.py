from pfb.cli import main
from click.testing import CliRunner

runner = CliRunner()
result = runner.invoke(
    main,
    ["show", "-i", "test.pfb", "gen3metadata"],
)
print(result.output)
