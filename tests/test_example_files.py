from projectcard import read_cards
from projectcard.validate import ProjectCardValidationError

from network_wrangler.logger import WranglerLogger


def test_example_project_cards_valid(request, stpaul_card_dir):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    cards = read_cards(stpaul_card_dir)
    errors = []
    ok = []
    for project, card in cards.items():
        WranglerLogger.debug(f"Evaluating: {project}")
        try:
            assert card.valid
        except ProjectCardValidationError as e:
            errors.append(project)
            WranglerLogger.error(e)
        except AssertionError as e:
            errors.append(project)
            WranglerLogger.error(e)
        else:
            ok.append(project)
    _delim = "\n - "
    WranglerLogger.debug(f"Portion Valid: {len(ok)}/{len(cards)}")
    WranglerLogger.debug(f"Valid Cards: {_delim}{_delim.join(ok)}")
    if errors:
        WranglerLogger.error(f"Card Validation Errors: {_delim}{_delim.join(errors)}")
        raise ProjectCardValidationError(
            f"Errors in {len(errors)} of {len(cards)} example project cards"
        )

    WranglerLogger.info(f"Evaluated {len(cards)} card files")
    WranglerLogger.info(f"--Finished: {request.node.name}")


def test_bad_project_cards_fail(request, bad_project_cards):
    WranglerLogger.info(f"--Starting: {request.node.name}")
    for s in bad_project_cards:
        try:
            cards = read_cards(s)
            [c.validate() for c in cards.values()]
        except ProjectCardValidationError:
            pass
        else:
            WranglerLogger.error(f"Schema should not be valid: {s}")
            raise ValueError(
                "Schema shouldn't be valid but is not raising an error in validate_schema_file"
            )
    WranglerLogger.info(f"--Finished: {request.node.name}")
