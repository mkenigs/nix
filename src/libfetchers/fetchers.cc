#include "fetchers.hh"
#include "store-api.hh"
#include "eval-inline.hh"

#include <nlohmann/json.hpp>

namespace nix::fetchers {

std::unique_ptr<std::vector<std::unique_ptr<InputScheme>>> inputSchemes = nullptr;

void registerInputScheme(std::unique_ptr<InputScheme> && inputScheme)
{
    if (!inputSchemes) inputSchemes = std::make_unique<std::vector<std::unique_ptr<InputScheme>>>();
    inputSchemes->push_back(std::move(inputScheme));
}

std::unique_ptr<Input> inputFromURL(const ParsedURL & url)
{
    for (auto & inputScheme : *inputSchemes) {
        auto res = inputScheme->inputFromURL(url);
        if (res) return res;
    }
    throw Error("input '%s' is unsupported", url.url);
}

std::unique_ptr<Input> inputFromURL(const std::string & url)
{
    return inputFromURL(parseURL(url));
}

std::unique_ptr<Input> inputFromAttrs(const Attrs & attrs)
{
    auto attrs2(attrs);
    attrs2.erase("narHash");
    for (auto & inputScheme : *inputSchemes) {
        auto res = inputScheme->inputFromAttrs(attrs2);
        if (res) {
            if (auto narHash = maybeGetStrAttr(attrs, "narHash"))
                // FIXME: require SRI hash.
                res->narHash = Hash(*narHash);
            return res;
        }
    }
    throw Error("input '%s' is unsupported", attrsToJson(attrs));
}

Attrs Input::toAttrs() const
{
    auto attrs = toAttrsInternal();
    if (narHash)
        attrs.emplace("narHash", narHash->to_string(SRI));
    attrs.emplace("type", type());
    return attrs;
}

Tree Input::substituteTree(EvalState & state) const
{
    assert(narHash);
    auto storePath = state.store->makeFixedOutputPath(true, narHash.value, "source");

    state.store->ensurePath(storePath);

    debug("using substituted/cached input '%s' in '%s'",
        to_string(), state.store->printStorePath(storePath));

    auto actualPath = state.store->toRealPath(storePath);

    if (state.allowedPaths)
        state.allowedPaths->insert(actualPath);

    return {
        Tree {
            .actualPath = actualPath,
            .storePath = std::move(storePath),
            .info = { narHash.value() }
        }
    };
}

std::pair<Tree, std::optional<std::shared_ptr<const Input>>> Input::fetchTree(EvalState & state) const
{
    if (narHash) {
        try {
            auto tree = substituteTree(state);
            return {std::move(tree), std::nullopt};
        } catch (Error & e) {
            debug("substitution of input '%s' failed: %s", to_string(), e.what());
        }
    }

    auto [tree, input] = fetchTreeInternal(state.store);

    if (tree.actualPath == "")
        tree.actualPath = store->toRealPath(tree.storePath);

    if (state.allowedPaths)
        state.allowedPaths->insert(tree.actualPath);

    if (!tree.info.narHash)
        tree.info.narHash = store->queryPathInfo(tree.storePath)->narHash;

    if (input->narHash)
        assert(input->narHash == tree.info.narHash);

    if (narHash && narHash != input->narHash)
        throw Error("NAR hash mismatch in input '%s' (%s), expected '%s', got '%s'",
            to_string(), tree.actualPath, narHash->to_string(SRI), input->narHash->to_string(SRI));

    return {std::move(tree), input};
}

std::shared_ptr<const Input> Input::applyOverrides(
    std::optional<std::string> ref,
    std::optional<Hash> rev) const
{
    if (ref)
        throw Error("don't know how to apply '%s' to '%s'", *ref, to_string());
    if (rev)
        throw Error("don't know how to apply '%s' to '%s'", rev->to_string(Base16, false), to_string());
    return shared_from_this();
}

}
