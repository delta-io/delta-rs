module.exports = {
    extends: ['@commitlint/config-conventional'],
    // Workaround for https://github.com/dependabot/dependabot-core/issues/5923
    ignores: [(message) => /^Bumps \[.+]\(.+\) from .+ to .+\.$/m.test(message)],
    rules: {
        'body-max-length': [0, 'always'],
        'body-max-line-length': [0, 'always'],
        'footer-max-length': [0, 'always'],
        'footer-max-line-length': [0, 'always'],
        'header-max-length': [0, 'always'],
        'scope-max-length': [0, 'always'],
        'subject-max-length': [0, 'always'],
        'type-max-length': [0, 'always'],
    },
}