"""Record which instrument each OpenHTF plug opened.

OpenHTF tears its plugs down before the output callbacks run, and its JSON
record keeps nothing about them. `record_plugs` wraps the test's phases in a
group whose teardown phase reads every plug while it is still open and stores
two things in the record's metadata, which `upload()` sends along:

* each plug's identity: the manufacturer, model, serial_number and firmware
  its `_asdict()` returns (any other key is ignored);
* the plugs each phase received, so every measurement is tied to the
  instruments that took it.

    test = htf.Test(record_plugs(power_up, measure_rail), procedure_id="...")
    test.add_output_callbacks(upload())

The teardown phase runs whether the test passes, fails or errors. It is
named `tofupilot_record_plugs` and does not appear in the run TofuPilot
creates.
"""

from typing import Any, Dict, List, Type

# Where the plugs go in `test_record.metadata`, and the name of the phase
# that writes them. The server reads the first and drops the second.
METADATA_KEY = "tofupilot_plugs"
PHASE_NAME = "tofupilot_record_plugs"


def _plug_path(plug_type: Type[Any]) -> str:
    # The name OpenHTF itself gives a plug in its station state.
    return f"{plug_type.__module__}.{plug_type.__name__}"


def _plug_state(plug: Any) -> Dict[str, Any]:
    try:
        state = plug._asdict()
    except Exception:  # A plug's _asdict() may talk to the instrument.
        return {}
    if not isinstance(state, dict):
        return {}
    # As text here: JSON would turn a 1.0 firmware into 1 on the way, where
    # the TofuPilot Framework records "1.0".
    return {
        key: str(value)
        for key, value in state.items()
        if key in ("manufacturer", "model", "serial_number", "firmware")
        and isinstance(value, (str, int, float))
        and not isinstance(value, bool)
    }


def record_plugs(*phases: Any) -> Any:
    """Wrap `phases` so the run records the instrument behind each plug.

    Takes what `htf.Test` takes (phase functions, phase descriptors, phase
    groups or sequences) and returns a phase group to pass to `htf.Test` in
    their place.
    """
    import openhtf as htf
    from openhtf.core import base_plugs, phase_collections

    main = phase_collections.PhaseSequence(phase_collections.flatten(phases))

    plug_types: List[Type[base_plugs.BasePlug]] = []
    phase_plugs: Dict[str, List[str]] = {}
    for phase in main.all_phases():
        paths = phase_plugs.setdefault(phase.name, [])
        for phase_plug in phase.plugs:
            if not isinstance(phase_plug.cls, type):
                # A PlugPlaceholder: the teardown phase must ask for the
                # real class, which with_plugs() only names later.
                raise ValueError(
                    f"record_plugs: plug '{phase_plug.name}' of phase '{phase.name}' is a placeholder. "
                    "Call with_plugs() on the phases first, then wrap them: "
                    "record_plugs(phase.with_plugs(...))."
                )
            if phase_plug.cls not in plug_types:
                plug_types.append(phase_plug.cls)
            path = _plug_path(phase_plug.cls)
            if path not in paths:
                paths.append(path)

    arg_names = {f"plug_{n}": plug_type for n, plug_type in enumerate(plug_types)}

    def tofupilot_record_plugs(test: Any, **plugs: Any) -> None:
        # Keyed by the class the phases declared, as phase_plugs is.
        paths = {name: _plug_path(arg_names[name]) for name in plugs}
        test.test_record.metadata[METADATA_KEY] = {
            "plug_descriptors": {paths[name]: {} for name in plugs},
            "plug_states": {paths[name]: _plug_state(plug) for name, plug in plugs.items()},
            "phase_plugs": phase_plugs,
        }

    capture = htf.plug(**arg_names)(htf.PhaseOptions(name=PHASE_NAME)(tofupilot_record_plugs))
    return htf.PhaseGroup(main=main, teardown=[capture])
